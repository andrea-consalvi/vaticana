"""e-codices PDF Downloader — single-instance web service."""
from __future__ import annotations

import concurrent.futures
import os
import re
import shutil
import tempfile
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse

import img2pdf
import requests
from flask import Flask, jsonify, render_template, request, send_file
from PIL import Image, ImageOps, UnidentifiedImageError

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 4096

MAX_IMAGES = 1100
MAX_IMAGE_BYTES = 65 * 1024 * 1024
MAX_TOTAL_BYTES = 1600 * 1024 * 1024
MAX_AGE_SECONDS = 60 * 60
MAX_DOWNLOADS = 1
WORKERS = 2
# Se la pagina non comunica più per 90 secondi, annulliamo il lavoro.
CLIENT_LEASE_SECONDS = 90

JOBS: dict[str, dict] = {}
LOCK = threading.Lock()
DEFAULT_URL = 'https://digi.vatlib.it/view/MSS_Reg.lat.310'
MANIFEST_HEADERS = {'User-Agent': 'VaticanaPDF/1.0 (scholarly image conversion)', 'Accept': 'application/json'}
IMAGE_HEADERS = {'User-Agent': 'VaticanaPDF/1.0 (scholarly image conversion)', 'Accept': 'image/jpeg,image/*;q=0.9,*/*;q=0.5'}


def is_allowed_url(value: str) -> bool:
    try:
        u = urlparse(value)
        return (u.scheme == 'https' and u.port in (None, 443)
                and u.username is None and u.password is None
                and (u.hostname or '').lower() == 'digi.vatlib.it')
    except ValueError:
        return False


def secure_url(value: str) -> str:
    if not is_allowed_url(value):
        raise ValueError('Il server ha restituito un indirizzo non autorizzato.')
    return value


def safe_get(url: str, *, timeout=(10, 45), stream=False, headers=None):
    current = secure_url(url)
    for _ in range(5):
        response = requests.get(current, headers=headers, timeout=timeout, stream=stream, allow_redirects=False)
        if response.status_code in (301, 302, 303, 307, 308):
            location = response.headers.get('Location')
            response.close()
            if not location:
                raise RuntimeError('Reindirizzamento senza indirizzo.')
            current = secure_url(urljoin(current, location))
            continue
        return response
    raise RuntimeError('Troppi reindirizzamenti.')


def manuscript_id(raw: str) -> str:
    if not is_allowed_url(raw):
        raise ValueError('Incolla un link HTTPS di digi.vatlib.it.')
    path = urlparse(raw).path
    match = re.fullmatch(r'/(?:view|iiif)/((?:MSS_)[A-Za-z0-9._-]+)(?:/manifest\.json)?/?', path)
    if not match:
        raise ValueError('Link non riconosciuto. Usa https://digi.vatlib.it/view/MSS_Reg.lat.310')
    return match.group(1)


def fetch_manifest(mid: str):
    url = f'https://digi.vatlib.it/iiif/{mid}/manifest.json'
    with safe_get(url, timeout=(12, 60), headers=MANIFEST_HEADERS) as r:
        r.raise_for_status()
        if int(r.headers.get('content-length', 0)) > 16_000_000:
            raise RuntimeError('Manifest troppo grande.')
        raw = r.content
        if len(raw) > 16_000_000:
            raise RuntimeError('Manifest troppo grande.')
        manifest = r.json()
        if not isinstance(manifest, dict):
            raise ValueError('Manifest non valido.')
        return manifest


def service_and_direct(body):
    if isinstance(body, list):
        body = next((x for x in body if isinstance(x, dict)), {})
    if not isinstance(body, dict):
        return None
    services = body.get('service') or []
    if isinstance(services, dict):
        services = [services]
    for service in services:
        if isinstance(service, dict):
            base = service.get('id') or service.get('@id')
            if base and is_allowed_url(base):
                return ('iiif', secure_url(base).rstrip('/'))
    direct = body.get('id') or body.get('@id')
    if direct and is_allowed_url(direct):
        return ('direct', secure_url(direct))
    return None


def images_from_manifest(manifest):
    images = []
    # IIIF Presentation 2: first sequence is the physical canvas order
    sequences = manifest.get('sequences') or []
    if sequences:
        for canvas in sequences[0].get('canvases', []):
            for annotation in canvas.get('images', []):
                item = service_and_direct(annotation.get('resource'))
                if item:
                    images.append(item)
                    break
    # IIIF Presentation 3
    else:
        for canvas in manifest.get('items', []):
            found = False
            for page in canvas.get('items', []):
                for annotation in page.get('items', []):
                    if annotation.get('motivation') not in (None, 'painting', ['painting']):
                        continue
                    item = service_and_direct(annotation.get('body'))
                    if item:
                        images.append(item)
                        found = True
                        break
                if found:
                    break
    if not images:
        raise RuntimeError('Il manifest non contiene immagini IIIF compatibili o indica immagini su un server non ammesso.')
    if len(images) > MAX_IMAGES:
        raise RuntimeError(f'Il manoscritto contiene {len(images)} immagini, oltre il limite di sicurezza ({MAX_IMAGES}).')
    return images


def image_candidates(image, quality):
    kind, source = image
    if kind == 'direct':
        return [source]
    if quality == 'full':
        sizes = ('max', 'full')
    else:
        # IIIF v2 square bounding box; width-only alternative for older Loris installations
        sizes = (f'!{quality},{quality}', f'{quality},', f'w{quality},')
    return [f'{source}/full/{size}/0/default.jpg' for size in sizes]


class JobCancelled(Exception):
    """A job was explicitly cancelled or its browser tab stopped checking in."""


def ensure_active(job_id: str):
    with LOCK:
        job = JOBS.get(job_id)
        if not job or job['cancel_event'].is_set() or job['status'] == 'cancelled':
            raise JobCancelled('Download annullato.')
        if (job['status'] == 'running'
                and time.monotonic() - job['last_seen'] > CLIENT_LEASE_SECONDS):
            job['cancel_event'].set()
            job['status'] = 'cancelled'
            job['message'] = 'Download interrotto: la pagina non è più collegata.'
            raise JobCancelled(job['message'])


def download_one(index: int, image, quality: str, folder: Path, job_id: str):
    """Stream a single image with periodic cooperative cancellation checks."""
    last_error = None
    for url in image_candidates(image, quality):
        for attempt in range(2):
            ensure_active(job_id)
            tmp = folder / f'{index:05}.part'
            try:
                with safe_get(url, timeout=(12, 40), stream=True, headers=IMAGE_HEADERS) as r:
                    r.raise_for_status()
                    if int(r.headers.get('content-length', 0)) > MAX_IMAGE_BYTES:
                        raise RuntimeError('Immagine oltre il limite di 65 MB.')
                    received = 0
                    with tmp.open('wb') as stream:
                        for chunk in r.iter_content(chunk_size=131072):
                            ensure_active(job_id)
                            if not chunk:
                                continue
                            received += len(chunk)
                            if received > MAX_IMAGE_BYTES:
                                raise RuntimeError('Immagine oltre il limite di 65 MB.')
                            stream.write(chunk)
                ensure_active(job_id)
                with Image.open(tmp) as im:
                    im.verify()
                with Image.open(tmp) as im:
                    if im.format == 'JPEG' and im.mode in ('RGB', 'L'):
                        target = folder / f'{index:05}.jpg'
                        tmp.replace(target)
                        return target, target.stat().st_size
                    target = folder / f'{index:05}.jpg'
                    ImageOps.exif_transpose(im).convert('RGB').save(target, 'JPEG', quality=88)
                tmp.unlink(missing_ok=True)
                return target, target.stat().st_size
            except JobCancelled:
                tmp.unlink(missing_ok=True)
                raise
            except (requests.RequestException, RuntimeError, OSError, UnidentifiedImageError, ValueError) as exc:
                tmp.unlink(missing_ok=True)
                last_error = exc
                if attempt == 0:
                    ensure_active(job_id)
                    time.sleep(0.7)
    ensure_active(job_id)
    raise RuntimeError(f'Errore durante il download della pagina {index + 1}: {last_error}')


def update(job_id: str, **changes):
    with LOCK:
        if job_id in JOBS:
            JOBS[job_id].update(changes)


def run_job(job_id: str, mid: str, quality: str):
    folder = None
    futures = {}
    try:
        ensure_active(job_id)
        update(job_id, message='Lettura del manifest IIIF…')
        manifest = fetch_manifest(mid)
        ensure_active(job_id)
        images = images_from_manifest(manifest)
        folder = Path(tempfile.mkdtemp(prefix='vaticana_pdf_'))
        update(job_id, total=len(images), message=f'{len(images)} immagini individuate. Scaricamento avviato…')
        paths = [None] * len(images)
        total_bytes = 0
        completed = 0
        # Wait in short intervals to detect closed tabs also when the image server is slow.
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(download_one, i, item, quality, folder, job_id): i
                       for i, item in enumerate(images)}
            pending = set(futures)
            try:
                while pending:
                    ensure_active(job_id)
                    finished, pending = concurrent.futures.wait(
                        pending, timeout=1.0, return_when=concurrent.futures.FIRST_COMPLETED)
                    for future in finished:
                        ensure_active(job_id)
                        path, nbytes = future.result()
                        paths[futures[future]] = path
                        total_bytes += nbytes
                        completed += 1
                        if total_bytes > MAX_TOTAL_BYTES:
                            raise RuntimeError('Limite di spazio raggiunto (1600 MB). Prova una qualità inferiore.')
                        update(job_id, percent=int(completed / len(images) * 92),
                               message=f'Immagini scaricate: {completed}/{len(images)}')
            except Exception:
                for future in futures:
                    future.cancel()
                # Cancellation is propagated to active workers; the executor waits for
                # them to stop before the temporary folder can safely be removed.
                raise
        ensure_active(job_id)
        update(job_id, percent=94, message='Assemblaggio del PDF senza ricompressione…')
        output_path = folder / f'{mid}.pdf'
        with output_path.open('wb') as output:
            img2pdf.convert([str(p) for p in paths], outputstream=output)
        ensure_active(job_id)
        for image_path in paths:
            image_path.unlink(missing_ok=True)
        update(job_id, status='done', percent=100,
               message=f'PDF pronto: {len(paths)} immagini, {output_path.stat().st_size / (1024 ** 2):.1f} MB.',
               path=str(output_path), filename=output_path.name)
    except JobCancelled:
        update(job_id, status='cancelled', message='Download annullato.', percent=0)
        if folder:
            shutil.rmtree(folder, ignore_errors=True)
    except Exception as exc:
        with LOCK:
            cancelled = job_id in JOBS and JOBS[job_id]['cancel_event'].is_set()
        if cancelled:
            update(job_id, status='cancelled', message='Download annullato.', percent=0)
        else:
            update(job_id, status='error', message=str(exc), percent=0)
        if folder:
            shutil.rmtree(folder, ignore_errors=True)


def cleanup():
    now = time.time()
    with LOCK:
        expired = [job_id for job_id, job in JOBS.items()
                   if job['status'] != 'running' and now - job['created_at'] > MAX_AGE_SECONDS]
        deleted = [JOBS.pop(job_id) for job_id in expired]
    for job in deleted:
        if job.get('path'):
            shutil.rmtree(Path(job['path']).parent, ignore_errors=True)


@app.get('/')
def index():
    return render_template('index.html', example=DEFAULT_URL)


@app.get('/health')
def health():
    return jsonify(status='ok')


@app.post('/api/jobs')
def create_job():
    cleanup()
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify(error='Richiesta non valida.'), 400
    try:
        mid = manuscript_id(str(data.get('url', '')).strip())
        quality = str(data.get('quality', '2000'))
        if quality not in ('1200', '2000', '3000', 'full'):
            raise ValueError('Risoluzione non valida.')
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    with LOCK:
        if sum(j['status'] == 'running' for j in JOBS.values()) >= MAX_DOWNLOADS:
            return jsonify(error='È già in corso un download. Riprova quando sarà terminato.'), 429
        job_id = uuid.uuid4().hex
        JOBS[job_id] = {'status': 'running', 'message': 'Preparazione…', 'percent': 0,
                        'created_at': time.time(), 'last_seen': time.monotonic(),
                        'cancel_event': threading.Event(), 'total': 0}
    threading.Thread(target=run_job, args=(job_id, mid, quality), daemon=True).start()
    return jsonify(id=job_id), 202


@app.get('/api/jobs/<job_id>')
def status(job_id):
    with LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify(error='Operazione non trovata o scaduta.'), 404
        if job['status'] == 'running':
            job['last_seen'] = time.monotonic()
        return jsonify({key: value for key, value in job.items()
                        if key not in ('path', 'filename', 'created_at', 'last_seen', 'cancel_event')})


@app.post('/api/jobs/<job_id>/cancel')
def cancel_job(job_id):
    """Mark cancellation immediately; workers stop cooperatively and clean files."""
    with LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify(error='Operazione non trovata.'), 404
        if job['status'] == 'running':
            job['cancel_event'].set()
            job['status'] = 'cancelled'
            job['message'] = 'Download annullato.'
            job['percent'] = 0
        return jsonify(status=job['status'])


@app.get('/api/jobs/<job_id>/download')
def download(job_id):
    with LOCK:
        job = JOBS.get(job_id)
        if not job or job['status'] != 'done':
            return jsonify(error='Il PDF non è pronto o è scaduto.'), 404
        path, filename = job['path'], job['filename']
    if not Path(path).is_file():
        return jsonify(error='Il file non è più disponibile: ripeti il download.'), 410
    return send_file(path, mimetype='application/pdf', as_attachment=True, download_name=filename)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=int(os.environ.get('PORT', '5000')), debug=False)
