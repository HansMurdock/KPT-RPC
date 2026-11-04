(() => {
  'use strict';

  const CHUNK_SIZE = 64 * 1024;
  const FILTERS = {
    grayscale: { label: 'Grayscale', detail: 'Mode L', css: 'grayscale(1)' },
    blur: { label: 'Gaussian Blur', detail: 'Radius 3 px', css: 'blur(3px)' },
    sepia: { label: 'Sepia', detail: 'Warm tone', css: 'sepia(1)' },
    invert: { label: 'Invert', detail: 'Negative', css: 'invert(1)' },
    edge: { label: 'Edge Detect', detail: 'Find edges', css: '' },
    sharpen: { label: 'Sharpen', detail: 'Detail boost', css: '' }
  };
  const TERMINAL = new Set(['COMPLETED', 'FAILED', 'CANCELLED']);
  const state = {
    jobs: [], requestIndex: new Map(), events: [], rpcHistory: [],
    rpcCalls: 0, rpcSuccess: 0, rpcLatencyTotal: 0, dedupHits: 0,
    files: [], sampleFile: null, inputUrl: null, detailUrls: [],
    filter: 'grayscale', workerCount: 4, workerSlots: Array(8).fill(null),
    selectedJobId: null, monitoringJobId: null, uploading: false,
    benchmarkRows: [], benchmarkRunning: false
  };

  const $ = (id) => document.getElementById(id);
  const delay = (ms) => new Promise((resolve) => window.setTimeout(resolve, ms));
  const fmtBytes = (bytes) => {
    if (!Number.isFinite(bytes) || bytes <= 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB'];
    const power = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    return `${(bytes / (1024 ** power)).toFixed(power ? 1 : 0)} ${units[power]}`;
  };
  const fmtTime = (date) => new Intl.DateTimeFormat('id-ID', { hour: '2-digit', minute: '2-digit', second: '2-digit' }).format(date);
  const fmtClock = (date) => new Intl.DateTimeFormat('id-ID', { hour: '2-digit', minute: '2-digit' }).format(date);
  const uuid = () => window.crypto?.randomUUID?.() || 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = Math.random() * 16 | 0;
    return (c === 'x' ? r : (r & 3 | 8)).toString(16);
  });
  const jobId = () => `JOB-${uuid().replaceAll('-', '').slice(0, 8).toUpperCase()}`;
  const esc = (value) => String(value ?? '').replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
  const allJobs = () => state.jobs;
  const findJob = (id) => state.jobs.find((job) => job.id === id);

  async function makeDemoImage() {
    const canvas = document.createElement('canvas');
    canvas.width = 760;
    canvas.height = 470;
    const ctx = canvas.getContext('2d');
    const bg = ctx.createLinearGradient(0, 0, 760, 470);
    bg.addColorStop(0, '#13243a');
    bg.addColorStop(1, '#27303c');
    ctx.fillStyle = bg;
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    const colors = ['#e46662', '#eab953', '#62bd8e', '#579bd5', '#a17bd0', '#e18b58'];
    const stripe = canvas.width / colors.length;
    colors.forEach((color, index) => {
      ctx.fillStyle = color;
      ctx.fillRect(index * stripe, 55, stripe + 1, 230);
    });
    ctx.fillStyle = 'rgba(255,255,255,.92)';
    ctx.beginPath(); ctx.arc(380, 171, 93, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = '#438fbd';
    ctx.beginPath(); ctx.arc(380, 171, 58, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = '#f2f4f6';
    ctx.fillRect(32, 330, 696, 96);
    ctx.fillStyle = '#132033';
    ctx.font = '600 20px sans-serif';
    ctx.fillText('KPT-RPC · IMAGE PROCESSING QUEUE', 55, 371);
    ctx.font = '14px sans-serif';
    ctx.fillStyle = '#475569';
    ctx.fillText('Sample image · RGB input · 6 processing filters', 55, 399);
    const blob = await new Promise((resolve) => canvas.toBlob(resolve, 'image/png'));
    return new File([blob], 'kpt-demo.png', { type: 'image/png' });
  }

  function showToast(message, kind = 'success') {
    const toast = document.createElement('div');
    toast.className = `toast${kind === 'error' ? ' error' : ''}`;
    toast.textContent = message;
    $('toast-stack').append(toast);
    window.setTimeout(() => toast.remove(), 3500);
  }

  function addEvent(message, level = 'info') {
    state.events.unshift({ message, level, time: new Date() });
    state.events = state.events.slice(0, 32);
    renderActivity();
  }

  function recordRpc(method, type, result, latency = 0, detail = '') {
    state.rpcCalls += 1;
    if (result === 'OK' || result === 'STREAM') state.rpcSuccess += 1;
    state.rpcLatencyTotal += latency;
    state.rpcHistory.unshift({ method, type, result, latency, detail, time: new Date() });
    state.rpcHistory = state.rpcHistory.slice(0, 12);
    renderRpcTelemetry();
  }

  function setView(name) {
    document.querySelectorAll('.view').forEach((view) => view.classList.toggle('active', view.id === `view-${name}`));
    document.querySelectorAll('[data-view-link]').forEach((link) => {
      if (link.classList.contains('nav-link')) link.classList.toggle('active', link.dataset.viewLink === name);
    });
    const crumb = { overview: 'Overview', jobs: 'Job queue', benchmark: 'Benchmark', architecture: 'RPC architecture' };
    $('current-crumb').textContent = crumb[name] || 'Overview';
    if (name === 'jobs') renderJobsTable();
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }

  function updateFileSelection(files) {
    state.files = Array.from(files || []).filter((file) => file && file.type.startsWith('image/'));
    if (state.inputUrl) URL.revokeObjectURL(state.inputUrl);
    state.inputUrl = null;
    if (state.files.length) {
      const first = state.files[0];
      state.inputUrl = URL.createObjectURL(first);
      $('input-preview').src = state.inputUrl;
      $('selected-file-row').hidden = false;
      $('file-title').textContent = state.files.length === 1 ? first.name : `${state.files.length} images selected`;
      $('file-subtitle').innerHTML = '<em>Click to replace</em> · multiple images supported';
      $('selected-name').textContent = state.files.length === 1 ? first.name : `${state.files.length} image files`;
      const bytes = state.files.reduce((total, file) => total + file.size, 0);
      $('selected-info').textContent = state.files.length === 1
        ? `${fmtBytes(first.size)} · ${first.type || 'image'}`
        : `${fmtBytes(bytes)} total · batch upload`;
      $('upload-button-label').textContent = state.files.length === 1 ? 'Queue image job' : `Queue ${state.files.length} image jobs`;
      $('upload-button').disabled = state.uploading;
    } else {
      $('input-preview').removeAttribute('src');
      $('selected-file-row').hidden = true;
      $('file-title').textContent = 'Drop an image here';
      $('file-subtitle').innerHTML = 'or <em>browse files</em> · PNG, JPG, WEBP';
      $('upload-button-label').textContent = 'Choose an image first';
      $('upload-button').disabled = true;
    }
    $('image-input').value = '';
  }

  function statusClass(status) { return String(status || 'QUEUED').toLowerCase(); }
  function statusMarkup(status) { return `<span class="mini-status ${statusClass(status)}">${esc(status)}</span>`; }

  function renderStats() {
    const jobs = allJobs();
    const queued = jobs.filter((job) => job.status === 'QUEUED').length;
    const active = jobs.filter((job) => job.status === 'PROCESSING').length;
    const completed = jobs.filter((job) => job.status === 'COMPLETED').length;
    $('stat-total').textContent = String(jobs.length).padStart(2, '0');
    $('stat-queued').textContent = String(queued).padStart(2, '0');
    $('stat-workers').textContent = String(active).padStart(2, '0');
    $('stat-worker-cap').textContent = String(state.workerCount).padStart(2, '0');
    $('stat-completed').textContent = String(completed).padStart(2, '0');
    $('stat-success-copy').textContent = completed ? 'hasil siap diunduh' : 'belum ada output';
    $('nav-job-count').textContent = String(jobs.length);
    $('jobs-all-count').textContent = String(jobs.length);
    $('jobs-active-count').textContent = String(queued + active);
    $('jobs-failed-count').textContent = String(jobs.filter((job) => job.status === 'FAILED').length);
    $('dedup-count').textContent = String(state.dedupHits);
  }

  function renderFocus() {
    const job = findJob(state.selectedJobId);
    $('empty-job').hidden = Boolean(job);
    $('focus-job').hidden = !job;
    if (!job) return;
    $('focus-job-id').textContent = job.id;
    $('focus-filename').textContent = job.file.name;
    $('focus-filter').textContent = FILTERS[job.filter].label;
    $('focus-message').textContent = job.message;
    $('focus-percent').textContent = `${job.progress}%`;
    $('focus-bar').style.width = `${job.progress}%`;
    const badge = $('focus-status');
    badge.className = `status-badge ${statusClass(job.status)}`;
    badge.innerHTML = `<i></i> ${esc(job.status)}`;
    const steps = [...document.querySelectorAll('.timeline-step')];
    const stageIndex = job.status === 'QUEUED' ? 0 : job.status === 'PROCESSING' ? 1 : 2;
    steps.forEach((step, index) => {
      step.classList.remove('done', 'current', 'error');
      if (job.status === 'FAILED' || job.status === 'CANCELLED') {
        if (index === 0 && job.progress > 0) step.classList.add('done');
        if (index === 1) step.classList.add('error');
      } else if (index < stageIndex || job.status === 'COMPLETED') {
        step.classList.add('done');
      } else if (index === stageIndex) {
        step.classList.add('current');
      }
    });
    $('download-button').disabled = job.status !== 'COMPLETED';
    $('cancel-button').disabled = !['QUEUED', 'PROCESSING'].includes(job.status);
    $('monitor-button').textContent = state.monitoringJobId === job.id ? '◉ Stop stream' : '◉ Pantau stream';
  }

  function renderWorkers() {
    const root = $('worker-grid');
    const activeCount = state.workerSlots.slice(0, state.workerCount).filter(Boolean).length;
    root.innerHTML = Array.from({ length: state.workerCount }, (_, index) => {
      const assigned = state.workerSlots[index];
      const job = assigned ? findJob(assigned) : null;
      const busy = Boolean(job && job.status === 'PROCESSING');
      return `<div class="worker-card${busy ? ' busy' : ''}"><div class="worker-card-top"><b>WORKER ${String(index + 1).padStart(2, '0')}</b><span class="worker-state">${busy ? 'BUSY' : 'IDLE'}</span></div>${busy ? `<span class="worker-job">${esc(job.id)} · ${job.progress}%</span><div class="worker-busy-bar"><span style="width:${job.progress}%"></span></div>` : '<span class="worker-job">listening for jobs</span>'}</div>`;
    }).join('');
    $('queue-length').textContent = String(state.jobs.filter((job) => job.status === 'QUEUED').length);
    const queued = state.jobs.filter((job) => job.status === 'QUEUED').reverse().slice(0, 5);
    $('queue-dots').innerHTML = queued.length
      ? queued.map((job) => `<span class="queue-chip">${esc(job.id.slice(-8))}</span>`).join('')
      : '<span class="queue-empty">No pending jobs</span>';
    $('worker-count').value = String(state.workerCount);
    $('worker-count').disabled = activeCount > 0;
  }

  function renderRecent() {
    const recent = state.jobs.slice(0, 4);
    $('recent-list').innerHTML = recent.length ? recent.map((job) => `<div class="recent-row" data-open-job="${esc(job.id)}"><div class="recent-file"><strong>${esc(job.file.name)}</strong><span class="recent-job-id">${esc(job.id)}</span></div><span class="recent-filter">${esc(FILTERS[job.filter].label)}</span>${statusMarkup(job.status)}</div>`).join('') : '<div class="empty-inline">Job yang dikirim akan tampil di sini.</div>';
  }

  function renderRpcTelemetry() {
    $('rpc-total').textContent = String(state.rpcCalls);
    $('rpc-success').textContent = String(state.rpcSuccess);
    $('rpc-latency').textContent = state.rpcCalls ? `${Math.round(state.rpcLatencyTotal / state.rpcCalls)} ms` : '—';
    $('rpc-log-body').innerHTML = state.rpcHistory.length ? state.rpcHistory.slice(0, 5).map((rpc) => {
      const resultClass = rpc.result === 'OK' || rpc.result === 'STREAM' ? 'rpc-result-ok' : 'rpc-result-error';
      return `<tr><td>${esc(rpc.method)}</td><td>${esc(rpc.type)}</td><td class="${resultClass}">${esc(rpc.result)}</td><td>${Math.round(rpc.latency)} ms</td></tr>`;
    }).join('') : '<tr><td colspan="4" class="empty-table">Belum ada pemanggilan RPC.</td></tr>';
  }

  function renderActivity() {
    $('activity-feed').innerHTML = state.events.length ? state.events.slice(0, 5).map((event) => `<span class="activity-item"><span class="activity-time">${fmtTime(event.time)}</span> <span class="activity-event">${esc(event.message)}</span></span>`).join('') : '<span class="activity-empty">Menunggu event pertama...</span>';
  }

  function filteredJobs() {
    const query = $('job-search').value.trim().toLowerCase();
    const status = $('job-status-filter').value;
    const activeOnly = $('active-only').checked;
    return state.jobs.filter((job) => {
      const matchesQuery = !query || job.id.toLowerCase().includes(query) || job.file.name.toLowerCase().includes(query);
      const matchesStatus = status === 'all' || job.status === status;
      const matchesActive = !activeOnly || ['QUEUED', 'PROCESSING'].includes(job.status);
      return matchesQuery && matchesStatus && matchesActive;
    });
  }

  function renderJobsTable() {
    const jobs = filteredJobs();
    $('jobs-table-summary').textContent = `Menampilkan ${jobs.length} dari ${state.jobs.length} jobs`;
    $('jobs-table-body').innerHTML = jobs.length ? jobs.map((job) => {
      const cancelDisabled = !['QUEUED', 'PROCESSING'].includes(job.status);
      const downloadDisabled = job.status !== 'COMPLETED';
      return `<tr data-open-job="${esc(job.id)}"><td><div class="job-cell"><strong>${esc(job.id)}</strong><small>${esc(job.file.name)} · ${fmtBytes(job.file.size)}</small></div></td><td>${statusMarkup(job.status)}</td><td>${esc(FILTERS[job.filter].label)}</td><td><div class="table-progress"><div class="progress-track"><span style="width:${job.progress}%"></span></div><small>${job.progress}%</small></div></td><td>${fmtBytes(job.outputBytes || job.file.size)}</td><td>${fmtClock(job.createdAt)}</td><td><div class="table-actions"><button class="table-action" data-action="status" data-job="${esc(job.id)}">Status</button><button class="table-action" data-action="download" data-job="${esc(job.id)}" ${downloadDisabled ? 'disabled' : ''}>↓</button><button class="table-action" data-action="cancel" data-job="${esc(job.id)}" ${cancelDisabled ? 'disabled' : ''}>×</button></div></td></tr>`;
    }).join('') : '<tr><td colspan="7" class="empty-table">Tidak ada job yang cocok dengan filter ini.</td></tr>';
  }

  function renderJobDetail() {
    const job = findJob(state.selectedJobId);
    const detail = $('job-detail-content');
    const compare = $('image-compare');
    state.detailUrls.forEach((url) => URL.revokeObjectURL(url));
    state.detailUrls = [];
    if (!job) {
      detail.innerHTML = 'Pilih satu job dari tabel untuk melihat detailnya.';
      compare.innerHTML = '<div class="compare-empty"><span>▧</span><b>Hasil muncul setelah job selesai</b><small>Output dikirim lewat stream UnduhGambar.</small></div>';
      $('result-type').textContent = 'No output';
      return;
    }
    detail.innerHTML = `<div class="detail-list"><div class="detail-item"><span>Job ID</span><b>${esc(job.id)}</b></div><div class="detail-item"><span>Status</span><b>${esc(job.status)} · ${job.progress}%</b></div><div class="detail-item"><span>Request UUID</span><b>${esc(job.requestId)}</b></div><div class="detail-item"><span>Processing type</span><b>${esc(FILTERS[job.filter].label)}</b></div><div class="detail-item"><span>Input / output</span><b>${fmtBytes(job.file.size)} / ${job.outputBytes ? fmtBytes(job.outputBytes) : '—'}</b></div><div class="detail-item"><span>Attempts / chunks</span><b>${job.attempts} attempts · ${job.chunkCount} chunks</b></div><div class="detail-item"><span>Created</span><b>${new Date(job.createdAt).toLocaleString('id-ID')}</b></div><div class="detail-item"><span>Worker / message</span><b>${esc(job.workerName || 'Queued')} · ${esc(job.message)}</b></div></div>`;
    if (job.status === 'COMPLETED' && job.outputBlob) {
      const before = URL.createObjectURL(job.file);
      const after = URL.createObjectURL(job.outputBlob);
      state.detailUrls.push(before, after);
      compare.innerHTML = `<div class="compare-images"><div class="compare-image-box"><img src="${before}" alt="Gambar input"><span>INPUT · ${esc(job.file.name)}</span></div><div class="compare-image-box"><img src="${after}" alt="Hasil ${esc(FILTERS[job.filter].label)}"><span>OUTPUT · ${esc(FILTERS[job.filter].label)}</span></div></div>`;
      $('result-type').textContent = `${fmtBytes(job.outputBytes)} · PNG`;
    } else {
      compare.innerHTML = `<div class="compare-empty"><span>◌</span><b>${esc(job.status)} · ${job.progress}%</b><small>${esc(job.message)}</small></div>`;
      $('result-type').textContent = 'Processing';
    }
  }

  function renderAll() {
    renderStats();
    renderFocus();
    renderWorkers();
    renderRecent();
    renderJobsTable();
    renderJobDetail();
    renderRpcTelemetry();
    renderActivity();
  }

  function setFocusJob(job) {
    if (!job) return;
    state.selectedJobId = job.id;
    renderAll();
  }

  function setUploadProgress(percent, label, chunkLabel) {
    $('upload-progress-wrap').hidden = false;
    $('upload-progress-label').textContent = label;
    $('upload-progress-percent').textContent = `${percent}%`;
    $('upload-progress-bar').style.width = `${percent}%`;
    $('chunk-caption').textContent = chunkLabel;
  }

  async function streamUpload(file, requestId, attempt) {
    const totalChunks = Math.max(1, Math.ceil(file.size / CHUNK_SIZE));
    setUploadProgress(0, `Mengirim metadata · attempt ${attempt}`, `metadata → ${totalChunks} data chunks`);
    addEvent(`UploadGambar · metadata ${requestId.slice(0, 8)}…`);
    await delay(65);
    for (let index = 0; index < totalChunks; index += 1) {
      const start = index * CHUNK_SIZE;
      const end = Math.min(file.size, start + CHUNK_SIZE);
      const chunk = file.slice(start, end);
      // Membaca slice memastikan browser meniru pembagian payload seperti client gRPC.
      await chunk.arrayBuffer();
      const progress = Math.round(((index + 1) / totalChunks) * 90);
      setUploadProgress(progress, `Streaming chunk ${index + 1} / ${totalChunks}`, `${fmtBytes(end)} / ${fmtBytes(file.size)} · 64 KB chunks`);
      await delay(Math.min(55, 18 + Math.floor(chunk.size / 8192)));
    }
    setUploadProgress(100, 'Upload stream selesai · menunggu ACK', `${totalChunks} chunks · metadata tervalidasi`);
    await delay(95);
    return totalChunks;
  }

  async function enqueueOne(file, requestId, simulateTimeout, processingFilter) {
    const started = performance.now();
    const existingAtStart = state.requestIndex.get(requestId);
    const maxAttempts = simulateTimeout && !existingAtStart ? 3 : 1;
    let job = existingAtStart ? findJob(existingAtStart) : null;
    let attempts = 0;
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      attempts = attempt;
      const chunkCount = await streamUpload(file, requestId, attempt);
      const existing = state.requestIndex.get(requestId);
      if (existing) {
        job = findJob(existing);
        state.dedupHits += 1;
        const latency = performance.now() - started;
        recordRpc('UploadGambar', 'client→unary', 'OK', latency, 'deduplicated request_id');
        addEvent(`request_id duplikat · kembali ke ${existing}`);
        showToast(`Request ID sudah dikenal. Job ${existing} dipakai kembali.`);
        $('upload-progress-label').textContent = `Deduplicated · existing ${existing}`;
        setFocusJob(job);
        return job;
      }

      job = {
        id: jobId(), requestId, file, filter: processingFilter, status: 'QUEUED', progress: 10,
        message: `Gambar masuk antrean pemrosesan (${FILTERS[processingFilter].label})`,
        createdAt: new Date(), outputBlob: null, outputBytes: 0, chunkCount,
        attempts: attempt, workerName: '', cancelled: false
      };
      state.jobs.unshift(job);
      state.requestIndex.set(requestId, job.id);
      addEvent(`Job ${job.id} accepted · status QUEUED · ${FILTERS[job.filter].label}`);
      addEvent(`queue.put(${job.id}) · FIFO queue · ${chunkCount} chunks`);
      setFocusJob(job);
      pumpWorkers();

      if (simulateTimeout && attempt === 1) {
        recordRpc('UploadGambar', 'client→unary', 'TIMEOUT', performance.now() - started, 'response simulated as lost');
        addEvent('DEADLINE_EXCEEDED · ACK hilang setelah job didaftarkan');
        const backoff = 500 * (2 ** (attempt - 1));
        $('upload-progress-label').textContent = `Retry in ${(backoff / 1000).toFixed(1)}s · UUID tetap`;
        $('upload-progress-percent').textContent = '↻';
        await delay(backoff);
        continue;
      }

      job.attempts = attempt;
      recordRpc('UploadGambar', 'client→unary', 'OK', performance.now() - started, `${chunkCount} chunks`);
      addEvent(`UploadResponse · ${job.id} · STATUS_QUEUED`);
      $('upload-progress-label').textContent = `ACK diterima · ${job.id}`;
      $('upload-progress-percent').textContent = '100%';
      showToast(`Job ${job.id} masuk antrean.`);
      renderAll();
      return job;
    }
    return job;
  }

  async function submitFiles() {
    if (!state.files.length || state.uploading) return;
    state.uploading = true;
    $('upload-button').disabled = true;
    const files = [...state.files];
    const simulateTimeout = $('simulate-timeout').checked;
    const processingFilter = state.filter;
    const suppliedId = $('request-id').value.trim();
    try {
      for (let index = 0; index < files.length; index += 1) {
        const file = files[index];
        if (!file.size) {
          showToast(`${file.name} kosong dan tidak dapat diunggah.`, 'error');
          continue;
        }
        const requestId = files.length === 1 && suppliedId ? suppliedId : uuid();
        if (files.length > 1 && suppliedId) addEvent('Batch mode · setiap file menerima UUID terpisah');
        if (files.length === 1 && !suppliedId) $('request-id').value = requestId;
        if (files.length > 1) setView('overview');
        await enqueueOne(file, requestId, simulateTimeout, processingFilter);
      }
    } catch (error) {
      console.error(error);
      recordRpc('UploadGambar', 'client→unary', 'ERROR', 0, error.message);
      addEvent(`Upload gagal · ${error.message}`, 'error');
      showToast(`Upload gagal: ${error.message}`, 'error');
    } finally {
      state.uploading = false;
      $('upload-button').disabled = !state.files.length;
      renderAll();
    }
  }

  function pumpWorkers() {
    for (let index = 0; index < state.workerCount; index += 1) {
      if (state.workerSlots[index]) continue;
      const job = state.jobs.filter((candidate) => candidate.status === 'QUEUED' && !candidate.cancelled).sort((a, b) => a.createdAt - b.createdAt)[0];
      if (!job) continue;
      state.workerSlots[index] = job.id;
      processJob(job, index).finally(() => {
        if (state.workerSlots[index] === job.id) state.workerSlots[index] = null;
        renderAll();
        pumpWorkers();
      });
    }
    renderWorkers();
  }

  async function processJob(job, workerIndex) {
    job.workerName = `ImageWorker-${workerIndex + 1}`;
    job.status = 'PROCESSING';
    job.progress = 35;
    job.message = `[${job.workerName}] Membaca citra asli (${fmtBytes(job.file.size)})...`;
    addEvent(`${job.workerName} mengambil ${job.id} dari queue`);
    if (state.monitoringJobId === job.id) addEvent(`PantauStatus stream · PROCESSING 35% · ${job.id}`);
    renderAll();
    await delay(370 + Math.random() * 180);
    if (job.cancelled) return;

    job.progress = 70;
    job.message = `[${job.workerName}] Menerapkan ${FILTERS[job.filter].label}...`;
    if (state.monitoringJobId === job.id) addEvent(`PantauStatus stream · PROCESSING 70% · ${job.id}`);
    renderAll();
    await delay(350 + Math.random() * 180);
    if (job.cancelled) return;

    try {
      const output = await processImage(job.file, job.filter);
      if (job.cancelled) return;
      job.outputBlob = output;
      job.outputBytes = output.size;
      job.status = 'COMPLETED';
      job.progress = 100;
      job.message = `[${job.workerName}] Filter ${FILTERS[job.filter].label} selesai · ${fmtBytes(output.size)} output`;
      addEvent(`Job ${job.id} COMPLETED · output ${fmtBytes(output.size)}`);
      if (state.monitoringJobId === job.id) {
        addEvent(`PantauStatus stream · COMPLETED 100% · stream closed`);
        state.monitoringJobId = null;
      }
    } catch (error) {
      job.status = 'FAILED';
      job.progress = 0;
      job.message = `Gagal memproses gambar: ${error.message}`;
      addEvent(`Job ${job.id} FAILED · ${error.message}`, 'error');
      showToast(`Pemrosesan ${job.file.name} gagal. Coba format gambar lain.`, 'error');
    }
    renderAll();
  }

  async function processImage(file, filter) {
    const bitmap = await createImageBitmap(file);
    const scale = Math.min(1, 1500 / Math.max(bitmap.width, bitmap.height));
    const canvas = document.createElement('canvas');
    canvas.width = Math.max(1, Math.round(bitmap.width * scale));
    canvas.height = Math.max(1, Math.round(bitmap.height * scale));
    const ctx = canvas.getContext('2d', { willReadFrequently: filter === 'edge' || filter === 'sharpen' });
    if (filter === 'edge' || filter === 'sharpen') {
      ctx.drawImage(bitmap, 0, 0, canvas.width, canvas.height);
      convolve(canvas, filter);
    } else {
      ctx.filter = FILTERS[filter].css || 'none';
      ctx.drawImage(bitmap, 0, 0, canvas.width, canvas.height);
      ctx.filter = 'none';
    }
    bitmap.close?.();
    const output = await new Promise((resolve, reject) => canvas.toBlob((blob) => blob ? resolve(blob) : reject(new Error('Browser tidak dapat membuat output gambar.')), 'image/png'));
    return output;
  }

  function convolve(canvas, filter) {
    const ctx = canvas.getContext('2d', { willReadFrequently: true });
    const { width, height } = canvas;
    const source = ctx.getImageData(0, 0, width, height);
    const input = source.data;
    const output = new Uint8ClampedArray(input);
    const kernel = filter === 'edge' ? [-1, -1, -1, -1, 8, -1, -1, -1, -1] : [0, -1, 0, -1, 5, -1, 0, -1, 0];
    for (let y = 1; y < height - 1; y += 1) {
      for (let x = 1; x < width - 1; x += 1) {
        const pixel = (y * width + x) * 4;
        for (let channel = 0; channel < 3; channel += 1) {
          let value = 0;
          let kernelIndex = 0;
          for (let ky = -1; ky <= 1; ky += 1) {
            for (let kx = -1; kx <= 1; kx += 1) {
              const sourcePixel = ((y + ky) * width + (x + kx)) * 4;
              value += input[sourcePixel + channel] * kernel[kernelIndex++];
            }
          }
          output[pixel + channel] = filter === 'edge' ? Math.min(255, Math.abs(value)) : value;
        }
        output[pixel + 3] = input[pixel + 3];
      }
    }
    ctx.putImageData(new ImageData(output, width, height), 0, 0);
  }

  function checkJob(job) {
    if (!job) return;
    const started = performance.now();
    recordRpc('CekStatus', 'unary→unary', 'OK', 18 + Math.random() * 32, `${job.status} ${job.progress}%`);
    addEvent(`CekStatus · ${job.id} → ${job.status} (${job.progress}%)`);
    showToast(`${job.id}: ${job.status} · ${job.progress}%`);
    setFocusJob(job);
    void started;
  }

  function toggleMonitor(job) {
    if (!job) return;
    if (state.monitoringJobId === job.id) {
      state.monitoringJobId = null;
      addEvent(`PantauStatus stream dihentikan · ${job.id}`);
      showToast('Status stream dihentikan.');
    } else {
      state.monitoringJobId = job.id;
      recordRpc('PantauStatus', 'unary→stream', 'STREAM', 24 + Math.random() * 20, 'stream opened');
      addEvent(`PantauStatus stream terbuka · ${job.id}`);
      if (TERMINAL.has(job.status)) {
        addEvent(`PantauStatus stream · ${job.status} ${job.progress}% · stream closed`);
        state.monitoringJobId = null;
      }
      showToast(`Memantau status ${job.id}.`);
    }
    renderFocus();
  }

  function cancelJob(job) {
    if (!job || !['QUEUED', 'PROCESSING'].includes(job.status)) return;
    job.cancelled = true;
    job.status = 'CANCELLED';
    job.progress = 0;
    job.message = 'Pekerjaan dibatalkan oleh client.';
    state.jobs = state.jobs;
    recordRpc('BatalkanJob', 'unary→unary', 'OK', 16 + Math.random() * 28, job.id);
    addEvent(`BatalkanJob · ${job.id} → CANCELLED`);
    if (state.monitoringJobId === job.id) state.monitoringJobId = null;
    showToast(`${job.id} dibatalkan.`);
    renderAll();
  }

  function downloadJob(job) {
    if (!job || job.status !== 'COMPLETED' || !job.outputBlob) return;
    const chunkCount = Math.max(1, Math.ceil(job.outputBytes / CHUNK_SIZE));
    recordRpc('UnduhGambar', 'unary→stream', 'OK', 30 + Math.random() * 45, `${chunkCount} output chunks`);
    addEvent(`UnduhGambar · ${job.id} · stream ${chunkCount} chunks · ${fmtBytes(job.outputBytes)}`);
    const base = job.file.name.replace(/\.[^.]+$/, '');
    const anchor = document.createElement('a');
    anchor.href = URL.createObjectURL(job.outputBlob);
    anchor.download = `${base}-${job.filter}.png`;
    anchor.click();
    window.setTimeout(() => URL.revokeObjectURL(anchor.href), 1200);
    showToast(`Output ${FILTERS[job.filter].label} diunduh.`);
  }

  function handleJobTableClick(event) {
    const actionButton = event.target.closest('[data-action]');
    if (actionButton) {
      const job = findJob(actionButton.dataset.job);
      if (actionButton.dataset.action === 'status') checkJob(job);
      if (actionButton.dataset.action === 'download') downloadJob(job);
      if (actionButton.dataset.action === 'cancel') cancelJob(job);
      renderJobsTable();
      return;
    }
    const row = event.target.closest('[data-open-job]');
    if (row) setFocusJob(findJob(row.dataset.openJob));
  }

  async function refreshJobs() {
    await delay(65);
    recordRpc('ListJobs', 'unary→unary', 'OK', 14 + Math.random() * 26, `${state.jobs.length} records`);
    addEvent(`ListJobs · ${state.jobs.length} record(s) dikembalikan`);
    renderJobsTable();
    renderStats();
    showToast('Job registry diperbarui.');
  }

  function renderBenchmark(rows) {
    const chart = $('throughput-chart');
    const latency = $('latency-chart');
    if (!rows.length) return;
    const maxThroughput = Math.max(...rows.map((row) => row.throughput));
    chart.innerHTML = rows.map((row) => {
      const height = Math.max(5, row.throughput / maxThroughput * 126);
      return `<div class="bar-column"><span class="bar-value">${row.throughput.toFixed(1)}</span><div class="bar-fill" style="height:${height}px"></div><span class="bar-axis-label">${row.clients}</span></div>`;
    }).join('');
    const maxLatency = Math.max(...rows.map((row) => row.p95));
    latency.innerHTML = rows.map((row) => `<div class="latency-row"><b>${row.clients} client${row.clients > 1 ? 's' : ''}</b><div class="latency-bars"><span class="latency-average-bar" style="width:${Math.max(3, row.average / maxLatency * 100)}%"></span><span class="latency-p95-bar" style="width:${Math.max(3, row.p95 / maxLatency * 100)}%"></span></div><div class="latency-values"><span>AVG ${Math.round(row.average)} ms</span><span>P95 ${Math.round(row.p95)} ms</span></div></div>`).join('');
    $('benchmark-table-body').innerHTML = rows.map((row) => `<tr><td><b>${row.clients}</b></td><td>20</td><td>${(row.totalTime / 1000).toFixed(2)} s</td><td>${Math.round(row.average)} ms</td><td>${Math.round(row.min)} ms</td><td>${Math.round(row.p95)} ms</td><td>${Math.round(row.max)} ms</td><td>${row.throughput.toFixed(2)} job/s · ${(row.throughput * 4).toFixed(2)} RPC/s</td><td><span class="rpc-result-ok">0</span></td></tr>`).join('');
  }

  async function runBenchmark() {
    if (state.benchmarkRunning) return;
    state.benchmarkRunning = true;
    const button = $('run-benchmark');
    button.disabled = true;
    button.innerHTML = '<span>◌</span> Running model...';
    $('benchmark-table-body').innerHTML = '<tr><td colspan="9" class="empty-table">Menghitung skenario client concurrency...</td></tr>';
    const rows = [];
    const workerCount = 8; // benchmark/benchmark.py menggunakan 8 image workers
    const serviceMs = 225;
    for (const clients of [1, 5, 20]) {
      await delay(450);
      const slots = Math.min(clients, workerCount);
      const waves = Math.ceil(20 / slots);
      const jitter = () => (Math.random() - .5) * 32;
      const latencies = Array.from({ length: 20 }, (_, index) => {
        const wave = Math.floor(index / slots);
        return Math.max(90, serviceMs + wave * serviceMs * .14 + jitter());
      }).sort((a, b) => a - b);
      const average = latencies.reduce((sum, value) => sum + value, 0) / latencies.length;
      const min = latencies[0];
      const p95 = latencies[Math.ceil(latencies.length * .95) - 1];
      const max = latencies[latencies.length - 1];
      const totalTime = waves * serviceMs + 110 + Math.random() * 90;
      rows.push({ clients, average, min, p95, max, totalTime, throughput: 20 / (totalTime / 1000) });
      state.benchmarkRows = [...rows];
      renderBenchmark(rows);
    }
    state.benchmarkRunning = false;
    button.disabled = false;
    button.innerHTML = '<span>▶</span> Run again';
    showToast('Simulasi benchmark selesai · model 20 jobs per skenario.');
  }

  function resetSimulation() {
    if (state.uploading) {
      showToast('Tunggu upload selesai sebelum reset.', 'error');
      return;
    }
    state.jobs.forEach((job) => { job.cancelled = true; });
    state.jobs = [];
    state.requestIndex.clear();
    state.events = [];
    state.rpcHistory = [];
    state.rpcCalls = 0;
    state.rpcSuccess = 0;
    state.rpcLatencyTotal = 0;
    state.dedupHits = 0;
    state.workerSlots = Array(8).fill(null);
    state.selectedJobId = null;
    state.monitoringJobId = null;
    state.benchmarkRows = [];
    $('request-id').value = '';
    $('simulate-timeout').checked = false;
    $('upload-progress-wrap').hidden = true;
    $('throughput-chart').innerHTML = '<div class="chart-empty">Jalankan simulasi benchmark untuk melihat grafik.</div>';
    $('latency-chart').innerHTML = '<div class="chart-empty">Belum ada data model.</div>';
    $('benchmark-table-body').innerHTML = '<tr><td colspan="9" class="empty-table">Belum ada simulasi. Klik Run simulation.</td></tr>';
    renderAll();
    showToast('Simulation state direset.');
  }

  function bindEvents() {
    document.querySelectorAll('[data-view-link]').forEach((link) => link.addEventListener('click', (event) => {
      event.preventDefault();
      setView(link.dataset.viewLink);
    }));
    $('filter-grid').addEventListener('click', (event) => {
      const option = event.target.closest('[data-filter]');
      if (!option) return;
      state.filter = option.dataset.filter;
      document.querySelectorAll('.filter-option').forEach((button) => button.classList.toggle('selected', button === option));
    });
    $('image-input').addEventListener('change', (event) => updateFileSelection(event.target.files));
    $('clear-files').addEventListener('click', () => updateFileSelection([]));
    $('dropzone').addEventListener('dragover', (event) => { event.preventDefault(); $('dropzone').classList.add('dragging'); });
    $('dropzone').addEventListener('dragleave', () => $('dropzone').classList.remove('dragging'));
    $('dropzone').addEventListener('drop', (event) => { event.preventDefault(); $('dropzone').classList.remove('dragging'); updateFileSelection(event.dataTransfer.files); });
    $('new-uuid').addEventListener('click', () => { $('request-id').value = uuid(); $('request-id').focus(); });
    $('upload-button').addEventListener('click', submitFiles);
    $('worker-count').addEventListener('change', (event) => {
      state.workerCount = Number(event.target.value);
      addEvent(`Worker pool disetel ke ${state.workerCount} thread`);
      renderAll();
      pumpWorkers();
    });
    $('check-status-button').addEventListener('click', () => checkJob(findJob(state.selectedJobId)));
    $('monitor-button').addEventListener('click', () => toggleMonitor(findJob(state.selectedJobId)));
    $('download-button').addEventListener('click', () => downloadJob(findJob(state.selectedJobId)));
    $('cancel-button').addEventListener('click', () => cancelJob(findJob(state.selectedJobId)));
    $('open-jobs').addEventListener('click', () => setView('jobs'));
    $('recent-list').addEventListener('click', (event) => {
      const row = event.target.closest('[data-open-job]');
      if (row) { setFocusJob(findJob(row.dataset.openJob)); setView('jobs'); }
    });
    $('jobs-table-body').addEventListener('click', handleJobTableClick);
    $('refresh-jobs').addEventListener('click', refreshJobs);
    $('job-search').addEventListener('input', renderJobsTable);
    $('job-status-filter').addEventListener('change', renderJobsTable);
    $('active-only').addEventListener('change', renderJobsTable);
    $('run-benchmark').addEventListener('click', runBenchmark);
    $('reset-button').addEventListener('click', resetSimulation);
    $('help-button').addEventListener('click', () => $('help-dialog').showModal());
    $('close-help').addEventListener('click', () => $('help-dialog').close());
    $('close-help-bottom').addEventListener('click', () => $('help-dialog').close());
  }

  async function init() {
    bindEvents();
    state.sampleFile = await makeDemoImage();
    updateFileSelection([state.sampleFile]);
    renderAll();
    addEvent('Simulator ready · browser-local state · 4 image workers');
  }

  document.addEventListener('DOMContentLoaded', init, { once: true });
})();
