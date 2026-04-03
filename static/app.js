/* ═══════════════════════════════════════════════════════════════
   Social Publisher — Frontend Application
   ═══════════════════════════════════════════════════════════════ */

// ─── State ─────────────────────────────────────────────────────
let posts = [];
let header = [];
let config = {};
let currentView = 'posts';
let calendarDate = new Date();
let deleteRowNumber = null;
let deletePostId = null;
let editPostId = null;

// Pagination state
const PAGE_SIZE = 20;
let currentPage = 1;

// File ID visibility
let showFileIds = false;

// Drive browser state
let driveStack = [];       // [{folderId, name}] for breadcrumb
let selectedDriveFile = null;
let selectedDriveFiles = [];   // [{id, name}] for multi-select (carousel)

// Filter state
let filters = { status: '', network: '', dateFrom: '', dateTo: '', search: '' };

// Character limits
const CHAR_LIMITS = { ig: 2200, fb: 63206, gbp: 1500, li: 3000 };

// Polling state
let pollTimer = null;
let pollInFlight = false;
const POLL_INTERVAL = 15_000; // 15 seconds
let lastStatusMap = {};       // { postId: status }

// ─── Init ──────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {
  await loadConfig();
  await loadPosts();
  startStatusPolling();
});

async function loadConfig() {
  try {
    const resp = await fetch('/api/config');
    config = await resp.json();
  } catch (e) {
    console.error('Failed to load config:', e);
  }
}

// ═══════════════════════════════════════════════════════════════
//  Posts — CRUD
// ═══════════════════════════════════════════════════════════════

async function loadPosts(silent = false) {
  if (!silent) {
    showElement('posts-loading');
    hideElement('posts-empty');
    hideElement('posts-table-wrapper');

    // Hide mobile cards while loading to prevent stale data showing
    const cardsEl = document.getElementById('posts-cards');
    if (cardsEl) {
      cardsEl.classList.add('hidden');
      cardsEl.innerHTML = '';
    }
  }

  try {
    const resp = await fetch('/api/posts');
    const data = await resp.json();

    if (data.error) {
      showToast(data.error, 'error');
      return;
    }

    posts = data.posts || [];
    header = data.header || [];
    lastStatusMap = buildStatusMap(posts);
    if (!silent) currentPage = 1;
    renderPosts();
    updateStats();
    renderCalendar();
  } catch (e) {
    showToast('שגיאה בטעינת הפוסטים', 'error');
    console.error(e);
  } finally {
    hideElement('posts-loading');
  }
}

function getFilteredPosts() {
  return posts.filter(post => {
    const status = (post.status || '').toUpperCase();

    // Special meta-filter: failures = ERROR + PARTIAL
    if (filters.status === '_FAILURES') {
      if (status !== 'ERROR' && status !== 'PARTIAL') return false;
    } else if (filters.status && status !== filters.status) {
      return false;
    }

    if (filters.network && (post.network || '') !== filters.network) return false;

    if (filters.dateFrom || filters.dateTo) {
      const pDate = parseDate(post.publish_at);
      if (!pDate) return false;
      if (filters.dateFrom) {
        const from = new Date(filters.dateFrom + 'T00:00:00');
        if (pDate < from) return false;
      }
      if (filters.dateTo) {
        const to = new Date(filters.dateTo + 'T23:59:59');
        if (pDate > to) return false;
      }
    }

    if (filters.search) {
      const q = filters.search.toLowerCase();
      const inCaption = (post.caption || '').toLowerCase().includes(q);
      const inIg = (post.caption_ig || '').toLowerCase().includes(q);
      const inFb = (post.caption_fb || '').toLowerCase().includes(q);
      const inGbp = (post.caption_gbp || '').toLowerCase().includes(q);
      const inLi = (post.caption_li || '').toLowerCase().includes(q);
      if (!inCaption && !inIg && !inFb && !inGbp && !inLi) return false;
    }

    return true;
  });
}

function applyFilters() {
  filters.status = document.getElementById('filter-status').value;
  filters.network = document.getElementById('filter-network').value;
  filters.dateFrom = document.getElementById('filter-date-from').value;
  filters.dateTo = document.getElementById('filter-date-to').value;
  filters.search = document.getElementById('filter-search').value;
  currentPage = 1;
  renderPosts();
}

function clearFilters() {
  document.getElementById('filter-status').value = '';
  document.getElementById('filter-network').value = '';
  document.getElementById('filter-date-from').value = '';
  document.getElementById('filter-date-to').value = '';
  document.getElementById('filter-search').value = '';
  filters = { status: '', network: '', dateFrom: '', dateTo: '', search: '' };
  currentPage = 1;
  renderPosts();
}

// ─── Custom Logo Upload ──────────────────────────────────────
function handleLogoUpload(event) {
  const file = event.target.files[0];
  if (!file) return;
  event.target.value = '';
  const reader = new FileReader();
  reader.onload = function(e) {
    const img = new Image();
    img.onload = function() {
      const MAX = 128;
      const canvas = document.createElement('canvas');
      canvas.width = MAX;
      canvas.height = MAX;
      const ctx = canvas.getContext('2d');
      const size = Math.min(img.width, img.height);
      const sx = (img.width - size) / 2;
      const sy = (img.height - size) / 2;
      ctx.drawImage(img, sx, sy, size, size, 0, 0, MAX, MAX);
      const compressed = canvas.toDataURL('image/jpeg', 0.8);
      applyLogoImage(compressed);
      try { localStorage.setItem('sp-custom-logo', compressed); } catch (err) {}
    };
    img.src = e.target.result;
  };
  reader.readAsDataURL(file);
}

function applyLogoImage(dataUrl) {
  const logo = document.getElementById('sidebar-logo');
  const img = document.getElementById('sidebar-logo-img');
  const text = document.getElementById('sidebar-logo-text');
  img.src = dataUrl;
  img.classList.remove('hidden');
  text.style.display = 'none';
  logo.classList.add('has-image');
}

// Restore custom logo on load
(function() {
  try {
    const saved = localStorage.getItem('sp-custom-logo');
    if (saved) applyLogoImage(saved);
  } catch (e) {}
})();

// ─── Collapsible Stats Bar ───────────────────────────────────
function toggleStatsBar() {
  const bar = document.getElementById('stats-bar');
  const collapsed = bar.classList.toggle('collapsed');
  try { localStorage.setItem('sp-stats-collapsed', collapsed ? '1' : '0'); } catch (e) {}
}

// Restore stats bar state on load
(function() {
  try {
    if (localStorage.getItem('sp-stats-collapsed') === '1') {
      document.getElementById('stats-bar').classList.add('collapsed');
    }
  } catch (e) {}
})();

// ─── Collapsible Filter Bar ──────────────────────────────────
function toggleFilterBar() {
  const bar = document.getElementById('filter-bar');
  const collapsed = bar.classList.toggle('collapsed');
  try { localStorage.setItem('sp-filter-collapsed', collapsed ? '1' : '0'); } catch (e) {}
}

// Restore filter bar state on load
(function() {
  try {
    if (localStorage.getItem('sp-filter-collapsed') === '1') {
      document.getElementById('filter-bar').classList.add('collapsed');
    }
  } catch (e) {}
})();

function renderPosts() {
  const tbody = document.getElementById('posts-tbody');
  const cardsEl = document.getElementById('posts-cards');
  const filtered = getFilteredPosts();

  if (filtered.length === 0) {
    if (posts.length === 0) {
      showElement('posts-empty');
    } else {
      hideElement('posts-empty');
    }
    hideElement('posts-table-wrapper');
    if (cardsEl) cardsEl.classList.add('hidden');
    removePagination();

    // Show "no results" only when filters are active but no posts match
    if (posts.length > 0 && filtered.length === 0) {
      showElement('posts-table-wrapper');
      tbody.innerHTML = `<tr><td colspan="12" style="text-align:center; padding:var(--space-2xl); color:var(--color-text-muted)">לא נמצאו פוסטים לפי הסינון הנוכחי</td></tr>`;
      if (cardsEl) {
        cardsEl.classList.remove('hidden');
        cardsEl.innerHTML = `<div class="post-card-empty">לא נמצאו פוסטים לפי הסינון הנוכחי</div>`;
      }
    }
    return;
  }

  hideElement('posts-empty');
  showElement('posts-table-wrapper');
  if (cardsEl) cardsEl.classList.remove('hidden');

  // Sort: newest first (by ID descending)
  const sorted = [...filtered].sort((a, b) => {
    const idA = parseInt(a.id, 10) || 0;
    const idB = parseInt(b.id, 10) || 0;
    return idB - idA;
  });

  // Pagination
  const totalPages = Math.ceil(sorted.length / PAGE_SIZE);
  if (currentPage > totalPages) currentPage = totalPages;
  if (currentPage < 1) currentPage = 1;
  const startIdx = (currentPage - 1) * PAGE_SIZE;
  const pageItems = sorted.slice(startIdx, startIdx + PAGE_SIZE);

  // Pre-compute shared values once per post
  const prepared = pageItems.map(post => {
    const status = (post.status || '').toUpperCase();
    const failedChannels = (post.failed_channels || '').split(',').map(s => s.trim()).filter(Boolean);
    const hasFailures = status === 'PARTIAL' || status === 'ERROR';
    return {
      post,
      status,
      badge: statusBadge(status),
      network: networkLabel(post.network),
      postType: postTypeLabel(post.post_type),
      publishAt: formatDateTime(post.publish_at),
      canEdit: status === 'READY' || status === 'DRAFT' || status === '',
      canDelete: status !== 'PROCESSING',
      channelResults: channelResultsHtml(post),
      failedChannels,
      hasFailures,
    };
  });

  // ── Desktop table ──
  tbody.innerHTML = prepared.map(({ post, badge, network, postType, publishAt, canEdit, canDelete, channelResults, failedChannels, hasFailures }) => {
    const captionIg = truncate(post.caption_ig, 40);
    const captionFb = truncate(post.caption_fb, 40);

    // Thumbnail + file name (support comma-separated multi-file IDs)
    const fileIds = (post.drive_file_id || '').split(',').map(s => s.trim()).filter(Boolean);
    const firstFileId = fileIds[0] || '';
    const thumbSrc = firstFileId ? `/api/drive/thumbnail/${encodeURIComponent(firstFileId)}` : '';
    const fileClickable = config.isDev && firstFileId;
    const fileClickAttr = fileClickable ? `onclick="openFileIdModal(this.dataset.fileIds)" data-file-ids="${escapeHtml(post.drive_file_id)}" style="cursor:pointer"` : '';
    const isMultiFile = fileIds.length > 1;
    const fileLabel = isMultiFile ? fileIds.length + ' קבצים' : truncate(firstFileId, 14);
    const fileTextPart = (showFileIds || isMultiFile) ? `<span class="file-name-text" title="${escapeHtml(post.drive_file_id)}">${fileLabel}</span>` : '';
    const fileCell = firstFileId
      ? `<div class="cell-file-preview" ${fileClickAttr}>
           <img class="file-thumbnail" src="${thumbSrc}" alt="" loading="lazy" onclick="event.stopPropagation(); openLightbox(this.src)" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex'">
           <span class="file-thumbnail-fallback" style="display:none">&#128247;</span>
           ${fileTextPart}
         </div>`
      : '<span style="color:var(--color-text-muted)">-</span>';

    // Single retry button for all failed channels
    let retryHtml = '';
    if (hasFailures && failedChannels.length > 0) {
      retryHtml = `<div class="retry-actions"><button class="btn btn-retry-all btn-sm" onclick="retryAllFailed(${post._row})" title="נסה שוב את כל הערוצים שנכשלו">&#8635; נסה שוב</button></div>`;
    }

    // Results cell: channel statuses + retry
    const resultsCell = channelResults || retryHtml
      ? `${channelResults}${retryHtml}`
      : '<span style="color:var(--color-text-muted)">-</span>';

    return `<tr>
      <td>${escapeHtml(post.id || '')}</td>
      <td>${badge}</td>
      <td>${network}</td>
      <td>${postType}</td>
      <td style="direction:ltr; text-align:start">${publishAt}</td>
      <td class="cell-caption ${post.caption_ig ? 'cell-clickable' : ''}" ${post.caption_ig ? `onclick="openCaptionModal('קפשן IG', this.dataset.full)" data-full="${escapeHtml(post.caption_ig)}"` : ''} title="${escapeHtml(post.caption_ig || '')}">${captionIg}</td>
      <td class="cell-caption ${post.caption_fb ? 'cell-clickable' : ''}" ${post.caption_fb ? `onclick="openCaptionModal('קפשן FB', this.dataset.full)" data-full="${escapeHtml(post.caption_fb)}"` : ''} title="${escapeHtml(post.caption_fb || '')}">${captionFb}</td>
      <td class="cell-caption ${post.caption_gbp ? 'cell-clickable' : ''}" ${post.caption_gbp ? `onclick="openCaptionModal('קפשן GBP', this.dataset.full)" data-full="${escapeHtml(post.caption_gbp)}"` : ''} title="${escapeHtml(post.caption_gbp || '')}">${truncate(post.caption_gbp, 40)}</td>
      <td class="cell-caption ${post.caption_li ? 'cell-clickable' : ''}" ${post.caption_li ? `onclick="openCaptionModal('קפשן LI', this.dataset.full)" data-full="${escapeHtml(post.caption_li)}"` : ''} title="${escapeHtml(post.caption_li || '')}">${truncate(post.caption_li, 40)}</td>
      <td class="cell-file">${fileCell}</td>
      <td class="cell-results">${resultsCell}</td>
      <td class="cell-actions">
        ${canEdit ? `<button class="btn btn-ghost btn-sm" onclick="openEditModal(${post._row})" title="עריכה">&#9998;</button>` : ''}
        <button class="btn btn-ghost btn-sm" onclick="duplicatePost(${post._row})" title="שכפול">&#128203;</button>
        ${canDelete ? `<button class="btn btn-ghost btn-sm" onclick="openDeleteConfirm(${post._row}, '${escapeHtml(post.id || '')}')" title="מחיקה" style="color:var(--color-error)">&#128465;</button>` : ''}
        ${post.error ? `<button class="btn btn-ghost btn-sm" onclick="showError(${post._row})" title="פרטי שגיאה" style="color:var(--color-warning)">&#9888;</button>` : ''}
      </td>
    </tr>`;
  }).join('');

  // ── Mobile cards ──
  if (cardsEl) {
    cardsEl.innerHTML = prepared.map(({ post, badge, network, postType, publishAt, canEdit, canDelete, channelResults, failedChannels, hasFailures }) => {
      const mFileIds = (post.drive_file_id || '').split(',').map(s => s.trim()).filter(Boolean);
      const mFirstId = mFileIds[0] || '';
      const mIsMulti = mFileIds.length > 1;
      const filePart = mFirstId
        ? `<div class="post-card-divider"></div>
           <div class="post-card-row">
             <span class="post-card-label">קובץ</span>
             <div class="post-card-file">
               <img src="/api/drive/thumbnail/${encodeURIComponent(mFirstId)}" alt="" loading="lazy" onclick="openLightbox(this.src)" onerror="this.style.display='none'">
               ${(mIsMulti || config.isDev) ? `<span>${mIsMulti ? mFileIds.length + ' קבצים' : truncate(mFirstId, 20)}</span>` : ''}
             </div>
           </div>`
        : '';

      const captionIgPart = post.caption_ig
        ? `<div class="post-card-divider"></div>
           <div>
             <span class="post-card-label">קפשן IG</span>
             <div class="post-card-caption" onclick="openCaptionModal('קפשן IG', this.dataset.full)" data-full="${escapeHtml(post.caption_ig)}">${escapeHtml(post.caption_ig)}</div>
           </div>`
        : '';

      const captionFbPart = post.caption_fb
        ? `<div class="post-card-divider"></div>
           <div>
             <span class="post-card-label">קפשן FB</span>
             <div class="post-card-caption" onclick="openCaptionModal('קפשן FB', this.dataset.full)" data-full="${escapeHtml(post.caption_fb)}">${escapeHtml(post.caption_fb)}</div>
           </div>`
        : '';

      const captionGbpPart = post.caption_gbp
        ? `<div class="post-card-divider"></div>
           <div>
             <span class="post-card-label">קפשן GBP</span>
             <div class="post-card-caption" onclick="openCaptionModal('קפשן GBP', this.dataset.full)" data-full="${escapeHtml(post.caption_gbp)}">${escapeHtml(post.caption_gbp)}</div>
           </div>`
        : '';

      const captionLiPart = post.caption_li
        ? `<div class="post-card-divider"></div>
           <div>
             <span class="post-card-label">קפשן LI</span>
             <div class="post-card-caption" onclick="openCaptionModal('קפשן LI', this.dataset.full)" data-full="${escapeHtml(post.caption_li)}">${escapeHtml(post.caption_li)}</div>
           </div>`
        : '';

      // Channel results + retry for mobile
      let resultsPart = '';
      const hasRetryButtons = hasFailures && failedChannels.length > 0;
      if (channelResults || hasRetryButtons) {
        let retryMobileHtml = '';
        if (hasRetryButtons) {
          retryMobileHtml = `<div class="retry-actions"><button class="btn btn-retry-all btn-sm" onclick="retryAllFailed(${post._row})">&#8635; נסה שוב</button></div>`;
        }
        resultsPart = `<div class="post-card-divider"></div>
          <div>
            <span class="post-card-label">תוצאות</span>
            ${channelResults}${retryMobileHtml}
          </div>`;
      }

      return `<div class="post-card">
        <div class="post-card-row">
          <div>${badge}</div>
          <span class="post-card-value" style="color:var(--color-text-muted); font-size:var(--font-size-xs)">#${escapeHtml(post.id || '')}</span>
        </div>
        <div class="post-card-divider"></div>
        <div class="post-card-row">
          <span class="post-card-label">רשת</span>
          <span class="post-card-value">${network}</span>
        </div>
        <div class="post-card-divider"></div>
        <div class="post-card-row">
          <span class="post-card-label">סוג</span>
          <span class="post-card-value">${postType}</span>
        </div>
        <div class="post-card-divider"></div>
        <div class="post-card-row">
          <span class="post-card-label">תאריך פרסום</span>
          <span class="post-card-value" style="direction:ltr">${publishAt}</span>
        </div>
        ${captionIgPart}
        ${captionFbPart}
        ${captionGbpPart}
        ${captionLiPart}
        ${filePart}
        ${resultsPart}
        <div class="post-card-divider"></div>
        <div class="post-card-actions">
          ${canEdit ? `<button class="btn btn-ghost btn-sm" onclick="openEditModal(${post._row})" title="עריכה">&#9998; עריכה</button>` : ''}
          <button class="btn btn-ghost btn-sm" onclick="duplicatePost(${post._row})" title="שכפול">&#128203; שכפול</button>
          ${canDelete ? `<button class="btn btn-ghost btn-sm" onclick="openDeleteConfirm(${post._row}, '${escapeHtml(post.id || '')}')" title="מחיקה" style="color:var(--color-error)">&#128465; מחיקה</button>` : ''}
          ${post.error ? `<button class="btn btn-ghost btn-sm" onclick="showError(${post._row})" title="פרטי שגיאה" style="color:var(--color-warning)">&#9888;</button>` : ''}
        </div>
      </div>`;
    }).join('');
  }

  // Pagination controls
  renderPagination(totalPages, sorted.length);
}

function removePagination() {
  document.querySelectorAll('.pagination').forEach(el => el.remove());
}

function renderPagination(totalPages, totalItems) {
  removePagination();

  if (totalPages <= 1) return;

  const html = `<div class="pagination">
    <button class="btn btn-ghost btn-sm" onclick="goToPage(${currentPage - 1})" ${currentPage <= 1 ? 'disabled' : ''}>&laquo; הקודם</button>
    <span class="pagination-info">${currentPage} / ${totalPages}</span>
    <button class="btn btn-ghost btn-sm" onclick="goToPage(${currentPage + 1})" ${currentPage >= totalPages ? 'disabled' : ''}>הבא &raquo;</button>
  </div>`;

  // Single pagination element at the end of the posts view
  const postsView = document.getElementById('view-posts');
  if (postsView) postsView.insertAdjacentHTML('beforeend', html);
}

function goToPage(page) {
  currentPage = page;
  renderPosts();
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

function updateStats() {
  const total = posts.length;
  const ready = posts.filter(p => (p.status || '').toUpperCase() === 'READY').length;
  const posted = posts.filter(p => (p.status || '').toUpperCase() === 'POSTED').length;
  const partial = posts.filter(p => (p.status || '').toUpperCase() === 'PARTIAL').length;
  const error = posts.filter(p => (p.status || '').toUpperCase() === 'ERROR').length;

  document.getElementById('stat-total').textContent = total;
  document.getElementById('stat-ready').textContent = ready;
  document.getElementById('stat-posted').textContent = posted;
  document.getElementById('stat-partial').textContent = partial;
  document.getElementById('stat-error').textContent = error;
}

// ─── Shared Form Setup ───────────────────────────────────────
// ─── Channel Checkboxes ↔ Post Type / GBP Fields Sync ────────
function getSelectedChannels() {
  const channels = [];
  if (document.getElementById('form-ch-ig').checked) channels.push('IG');
  if (document.getElementById('form-ch-fb').checked) channels.push('FB');
  if (document.getElementById('form-ch-gbp').checked) channels.push('GBP');
  if (document.getElementById('form-ch-li').checked) channels.push('LI');
  return channels;
}

function channelsToNetwork(channels) {
  const sorted = [...channels].sort((a, b) => {
    const order = { IG: 0, FB: 1, GBP: 2, LI: 3 };
    return (order[a] ?? 9) - (order[b] ?? 9);
  });
  return sorted.join('+') || '';
}

function networkToChannels(network) {
  if (!network) return ['IG', 'FB'];
  if (network === 'ALL') return ['IG', 'FB', 'GBP', 'LI'];
  return network.split('+').filter(Boolean);
}

function setChannelCheckboxes(channels) {
  document.getElementById('form-ch-ig').checked = channels.includes('IG');
  document.getElementById('form-ch-fb').checked = channels.includes('FB');
  document.getElementById('form-ch-gbp').checked = channels.includes('GBP');
  document.getElementById('form-ch-li').checked = channels.includes('LI');
}

function onChannelChange() {
  const channels = getSelectedChannels();
  const hasIG = channels.includes('IG');
  const hasFB = channels.includes('FB');
  const hasGBP = channels.includes('GBP');
  const hasLI = channels.includes('LI');
  const postTypeSelect = document.getElementById('form-post-type');
  const currentValue = postTypeSelect.value;

  // Post type: Reels only available when IG is selected
  if (!hasIG) {
    postTypeSelect.innerHTML = '<option value="FEED">תמונה / וידאו</option>';
    postTypeSelect.disabled = true;
  } else {
    postTypeSelect.innerHTML =
      '<option value="FEED">פיד (תמונה/וידאו)</option>' +
      '<option value="REELS">ריל (וידאו)</option>';
    postTypeSelect.disabled = false;
    postTypeSelect.value = currentValue;
  }

  // Show/hide per-channel caption groups
  document.getElementById('caption-ig-group').classList.toggle('hidden', !hasIG);
  document.getElementById('caption-fb-group').classList.toggle('hidden', !hasFB);

  // Show/hide GBP fields section
  document.getElementById('gbp-fields').classList.toggle('hidden', !hasGBP);

  // Show/hide LinkedIn fields section
  document.getElementById('li-fields').classList.toggle('hidden', !hasLI);
}

function onLiAuthorTypeChange() {
  const type = document.getElementById('form-li-author-type').value;
  const input = document.getElementById('form-li-author-urn');
  input.placeholder = type === 'person'
    ? 'urn:li:person:xxxxxxxxx'
    : 'urn:li:organization:xxxxxxxxx';
}

function onCtaTypeChange() {
  const ctaType = document.getElementById('form-cta-type').value;
  document.getElementById('cta-url-group').classList.toggle('hidden', !ctaType);
  if (!ctaType) {
    document.getElementById('form-cta-url').value = '';
  }
}

function toggleManualLocationId() {
  const el = document.getElementById('form-google-location-id-manual');
  el.classList.toggle('hidden');
  if (!el.classList.contains('hidden')) {
    el.focus();
  }
}

// Location loading: use a request counter to handle concurrent calls.
// Only the latest call applies its result, stale responses are ignored.
let _locationRequestId = 0;

function _setLocationLoading(loading) {
  const icon = document.getElementById('refresh-locations-icon');
  const btn = document.getElementById('btn-refresh-locations');
  if (loading) {
    icon.classList.add('spinning');
    btn.disabled = true;
  } else {
    icon.classList.remove('spinning');
    btn.disabled = false;
  }
}

function _showLocationWarning(message) {
  const warning = document.getElementById('location-warning');
  const text = document.getElementById('location-warning-text');
  if (message) {
    text.textContent = message;
    warning.classList.remove('hidden');
  } else {
    warning.classList.add('hidden');
    text.textContent = '';
  }
}

async function loadGbpLocations(selectLocationId = '', forceRefresh = false) {
  const requestId = ++_locationRequestId;
  const select = document.getElementById('form-google-location-id');
  const manual = document.getElementById('form-google-location-id-manual');
  _setLocationLoading(true);
  _showLocationWarning('');
  try {
    const url = forceRefresh ? '/api/gbp/locations?refresh=1' : '/api/gbp/locations';
    const resp = await fetch(url);
    const data = await resp.json();

    // Stale response — a newer call was made while this one was in flight
    if (requestId !== _locationRequestId) return;

    if (data.error || !data.locations) {
      select.innerHTML = '<option value="">בחר מיקום...</option>';
      _showLocationWarning('לא ניתן לטעון מיקומים מ-Google. ניתן להזין ידנית.');
      _applyLocationFallback(select, manual, selectLocationId);
      return;
    }

    if (data.locations.length === 0) {
      select.innerHTML = '<option value="">בחר מיקום...</option>';
      _showLocationWarning('לא נמצאו מיקומים בחשבון Google Business.');
      _applyLocationFallback(select, manual, selectLocationId);
      return;
    }

    // Keep the first "choose" option, add locations
    select.innerHTML = '<option value="">בחר מיקום...</option>';
    data.locations.forEach(loc => {
      const opt = document.createElement('option');
      opt.value = loc.name || loc.id;
      const title = loc.title || loc.name || loc.id;
      const address = loc.address || '';
      opt.textContent = address ? `${title} — ${address}` : title;
      select.appendChild(opt);
    });

    // Apply the location selection now that options are loaded
    if (selectLocationId) {
      const optionExists = Array.from(select.options).some(o => o.value === selectLocationId);
      if (optionExists) {
        select.value = selectLocationId;
        manual.value = '';
        manual.classList.add('hidden');
      } else {
        select.value = '';
        manual.value = selectLocationId;
        manual.classList.remove('hidden');
        _showLocationWarning('המיקום שנבחר לא נמצא ברשימת המיקומים הזמינים.');
      }
    }
  } catch (e) {
    console.error('Failed to load GBP locations:', e);
    if (requestId === _locationRequestId) {
      _showLocationWarning('שגיאה בטעינת מיקומים. ניתן להזין ידנית.');
      _applyLocationFallback(select, manual, selectLocationId);
    }
  } finally {
    if (requestId === _locationRequestId) {
      _setLocationLoading(false);
    }
  }
}

function refreshGbpLocations() {
  const select = document.getElementById('form-google-location-id');
  const currentValue = select.value ||
    document.getElementById('form-google-location-id-manual').value.trim();
  loadGbpLocations(currentValue, true);
}

function _applyLocationFallback(select, manual, locationId) {
  // If fetch failed but we have a location value, show it in the manual field
  // so the user doesn't lose the value.
  if (locationId) {
    select.value = '';
    manual.value = locationId;
    manual.classList.remove('hidden');
  }
}

function resetPostForm({ title, rowNumber = '', network = 'IG+FB', postType = 'FEED',
                         publishAt = '', caption = '', captionIg = '', captionFb = '',
                         captionGbp = '', captionLi = '', liAuthorUrn = '',
                         gbpPostType = 'STANDARD',
                         googleLocationId = '', ctaType = '', ctaUrl = '',
                         driveFileId = '', postId = null,
                         hashtags = '', firstComment = '' } = {}) {
  editPostId = postId;
  document.getElementById('post-modal-title').textContent = title;
  document.getElementById('form-row-number').value = rowNumber;

  // Set channel checkboxes and rebuild dropdown options BEFORE setting
  // post type — onChannelChange() rebuilds the post-type <select> options
  // based on selected channels, so it must run first to ensure the REELS
  // option exists when IG is selected.
  const channels = networkToChannels(network);
  setChannelCheckboxes(channels);
  onChannelChange();

  document.getElementById('form-post-type').value = postType;
  document.getElementById('form-publish-at').value = publishAt;
  document.getElementById('form-caption').value = caption;
  document.getElementById('form-caption-ig').value = captionIg;
  document.getElementById('form-caption-fb').value = captionFb;
  document.getElementById('form-caption-gbp').value = captionGbp;
  document.getElementById('form-caption-li').value = captionLi;
  document.getElementById('form-li-author-urn').value = liAuthorUrn;
  if (liAuthorUrn && liAuthorUrn.includes(':organization:')) {
    document.getElementById('form-li-author-type').value = 'organization';
  } else {
    document.getElementById('form-li-author-type').value = 'person';
  }
  onLiAuthorTypeChange();
  document.getElementById('form-gbp-post-type').value = gbpPostType || 'STANDARD';
  document.getElementById('form-cta-type').value = ctaType;
  document.getElementById('form-cta-url').value = ctaUrl;

  // Location: pre-populate the manual field as a fallback in case the
  // async loadGbpLocations() fetch fails. The async loader will move
  // the value to the dropdown if the option exists, or keep it in manual.
  document.getElementById('form-google-location-id').value = '';
  if (googleLocationId) {
    document.getElementById('form-google-location-id-manual').value = googleLocationId;
    document.getElementById('form-google-location-id-manual').classList.remove('hidden');
  } else {
    document.getElementById('form-google-location-id-manual').value = '';
    document.getElementById('form-google-location-id-manual').classList.add('hidden');
  }

  document.getElementById('form-hashtags').value = hashtags;
  document.getElementById('form-first-comment').value = firstComment;

  document.getElementById('form-drive-file-id').value = driveFileId;
  document.getElementById('form-drive-file-id-manual').value = '';

  if (driveFileId) {
    const fileCount = driveFileId.split(',').filter(s => s.trim()).length;
    const displayText = fileCount > 1 ? `${fileCount} קבצים נבחרו` : driveFileId;
    document.getElementById('selected-file-name').textContent = displayText;
    showElement('drive-file-display');
  } else {
    hideElement('drive-file-display');
  }

  hideElement('form-drive-file-id-manual');

  // Sync CTA URL visibility
  onCtaTypeChange();

  updateCharCounter('general');
  updateCharCounter('ig');
  updateCharCounter('fb');
  updateCharCounter('gbp');
  updateCharCounter('li');
  openModal('post-modal');
}

// ─── Create Post ─────────────────────────────────────────────
function openCreateModal() {
  loadGbpLocations('');
  resetPostForm({ title: 'פוסט חדש' });
}

// ─── Edit Post ───────────────────────────────────────────────
function openEditModal(rowNumber) {
  const post = posts.find(p => p._row === rowNumber);
  if (!post) return;

  // Convert publish_at to datetime-local format
  let publishAt = '';
  if (post.publish_at) {
    const dt = parseDate(post.publish_at);
    if (dt) {
      const pad = n => String(n).padStart(2, '0');
      publishAt = `${dt.getFullYear()}-${pad(dt.getMonth() + 1)}-${pad(dt.getDate())}T${pad(dt.getHours())}:${pad(dt.getMinutes())}`;
    }
  }

  loadGbpLocations(post.google_location_id || '');
  resetPostForm({
    title: 'עריכת פוסט',
    rowNumber,
    network: post.network || 'IG+FB',
    postType: post.post_type || 'FEED',
    publishAt,
    caption: post.caption || '',
    captionIg: post.caption_ig || '',
    captionFb: post.caption_fb || '',
    captionGbp: post.caption_gbp || '',
    captionLi: post.caption_li || '',
    liAuthorUrn: post.li_author_urn || '',
    gbpPostType: post.gbp_post_type || 'STANDARD',
    googleLocationId: post.google_location_id || '',
    ctaType: post.cta_type || '',
    ctaUrl: post.cta_url || '',
    hashtags: post.hashtags || '',
    firstComment: post.first_comment || '',
    driveFileId: post.drive_file_id || '',
    postId: post.id || null,
  });
}

// ─── Duplicate Post ─────────────────────────────────────────
function duplicatePost(rowNumber) {
  const post = posts.find(p => p._row === rowNumber);
  if (!post) return;

  loadGbpLocations(post.google_location_id || '');
  resetPostForm({
    title: 'שכפול פוסט',
    network: post.network || 'IG+FB',
    postType: post.post_type || 'FEED',
    caption: post.caption || '',
    captionIg: post.caption_ig || '',
    captionFb: post.caption_fb || '',
    captionGbp: post.caption_gbp || '',
    captionLi: post.caption_li || '',
    liAuthorUrn: post.li_author_urn || '',
    gbpPostType: post.gbp_post_type || 'STANDARD',
    googleLocationId: post.google_location_id || '',
    ctaType: post.cta_type || '',
    ctaUrl: post.cta_url || '',
    hashtags: post.hashtags || '',
    firstComment: post.first_comment || '',
    driveFileId: post.drive_file_id || '',
  });
}

// ─── Save Post (Create or Update) ───────────────────────────
async function savePost() {
  const rowNumber = document.getElementById('form-row-number').value;
  const publishAtInput = document.getElementById('form-publish-at').value;

  // Send the datetime as ISO 8601 with UTC offset so the backend can
  // convert to Israel time correctly regardless of the browser's timezone.
  let publishAt = '';
  if (publishAtInput) {
    const dt = new Date(publishAtInput);
    publishAt = dt.toISOString();
  }

  const channels = getSelectedChannels();
  const network = channelsToNetwork(channels);
  const hasGBP = channels.includes('GBP');
  const hasLI = channels.includes('LI');

  // Get google_location_id from select or manual input
  const locationSelect = document.getElementById('form-google-location-id').value;
  const locationManual = document.getElementById('form-google-location-id-manual').value.trim();
  const googleLocationId = locationSelect || locationManual;

  const data = {
    network: network,
    post_type: document.getElementById('form-post-type').value,
    publish_at: publishAt,
    caption: document.getElementById('form-caption').value,
    caption_ig: document.getElementById('form-caption-ig').value,
    caption_fb: document.getElementById('form-caption-fb').value,
    caption_gbp: document.getElementById('form-caption-gbp').value,
    caption_li: document.getElementById('form-caption-li').value,
    li_author_urn: document.getElementById('form-li-author-urn').value.trim(),
    gbp_post_type: document.getElementById('form-gbp-post-type').value,
    google_location_id: googleLocationId,
    cta_type: document.getElementById('form-cta-type').value,
    cta_url: document.getElementById('form-cta-url').value,
    hashtags: document.getElementById('form-hashtags').value,
    first_comment: document.getElementById('form-first-comment').value,
    drive_file_id: document.getElementById('form-drive-file-id').value,
  };

  // Include expected_id for concurrency-safe updates
  if (rowNumber && editPostId) {
    data.expected_id = editPostId;
  }

  // Validation
  if (channels.length === 0) {
    showToast('יש לבחור לפחות ערוץ אחד', 'error');
    return;
  }
  if (!data.publish_at) {
    showToast('יש לבחור תאריך ושעת פרסום', 'error');
    return;
  }
  // GBP and LI support text-only posts (no media required).
  // Only require media when at least one non-GBP/non-LI channel is selected.
  const needsMedia = channels.some(ch => ch !== 'GBP' && ch !== 'LI');
  if (needsMedia && !data.drive_file_id) {
    showToast('יש לבחור קובץ מדיה', 'error');
    return;
  }
  if (hasLI && !data.li_author_urn) {
    showToast('יש להזין LinkedIn Author URN', 'error');
    return;
  }
  if (hasGBP && !googleLocationId) {
    showToast('יש לבחור מיקום Google עבור GBP', 'error');
    return;
  }
  if (hasGBP && data.cta_type && !data.cta_url) {
    showToast('יש להזין כתובת URL עבור CTA', 'error');
    return;
  }
  if (hasGBP && !data.cta_type && data.cta_url) {
    showToast('יש לבחור סוג CTA כאשר מוזנת כתובת URL', 'error');
    return;
  }

  const btn = document.getElementById('btn-save-post');
  btn.disabled = true;
  btn.textContent = 'שומר...';

  try {
    let resp;
    if (rowNumber) {
      // Update
      resp = await fetch(`/api/posts/${rowNumber}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      });
    } else {
      // Create
      resp = await fetch('/api/posts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      });
    }

    const result = await resp.json();

    if (result.error) {
      showToast(result.error, 'error');
      return;
    }

    showToast(rowNumber ? 'הפוסט עודכן בהצלחה' : 'הפוסט נוצר בהצלחה', 'success');
    closePostModal();
    await loadPosts();

  } catch (e) {
    showToast('שגיאה בשמירת הפוסט', 'error');
    console.error(e);
  } finally {
    btn.disabled = false;
    btn.textContent = 'שמירה';
  }
}

function closePostModal() {
  closeModal('post-modal');
}

// ─── Delete Post ─────────────────────────────────────────────
function openDeleteConfirm(rowNumber, postId) {
  deleteRowNumber = rowNumber;
  deletePostId = postId;
  openModal('confirm-modal');
}

function closeConfirmModal() {
  closeModal('confirm-modal');
  deleteRowNumber = null;
  deletePostId = null;
}

async function confirmDelete() {
  if (!deleteRowNumber) return;

  const btn = document.getElementById('btn-confirm-delete');
  btn.disabled = true;
  btn.textContent = 'מוחק...';

  try {
    const deleteUrl = deletePostId
      ? `/api/posts/${deleteRowNumber}?expected_id=${encodeURIComponent(deletePostId)}`
      : `/api/posts/${deleteRowNumber}`;
    const resp = await fetch(deleteUrl, { method: 'DELETE' });
    const result = await resp.json();

    if (result.error) {
      showToast(result.error, 'error');
      return;
    }

    showToast('הפוסט נמחק', 'success');
    closeConfirmModal();
    await loadPosts();

  } catch (e) {
    showToast('שגיאה במחיקת הפוסט', 'error');
    console.error(e);
  } finally {
    btn.disabled = false;
    btn.textContent = 'מחיקה';
  }
}

// ─── Show Error Details ──────────────────────────────────────
function showError(rowNumber) {
  const post = posts.find(p => p._row === rowNumber);
  if (!post) return;

  const friendly = friendlyError(post.error);
  const detail = post.error || 'אין פרטי שגיאה';
  // Only show friendly + technical split if we found a known error code
  const text = friendly
    ? `${friendly}\n\n── פרטים טכניים ──\n${detail}`
    : detail;

  openCaptionModal(`שגיאה בפוסט #${post.id}`, text);
}

// ─── Caption Preview Modal ───────────────────────────────────
function openCaptionModal(title, text) {
  document.getElementById('caption-modal-title').textContent = title;
  document.getElementById('caption-modal-text').textContent = text;
  openModal('caption-modal');
}

function openFileIdModal(driveFileId) {
  if (!config.isDev) return;
  const ids = driveFileId.split(',').map(s => s.trim()).filter(Boolean);
  const displayText = ids.join('\n');
  document.getElementById('caption-modal-title').textContent = ids.length > 1 ? `File IDs (${ids.length})` : 'File ID';
  document.getElementById('caption-modal-text').textContent = displayText;
  openModal('caption-modal');
}

function closeCaptionModal() {
  closeModal('caption-modal');
}

// ─── Image Lightbox ──────────────────────────────────────────
function openLightbox(src) {
  const lightbox = document.getElementById('image-lightbox');
  const img = document.getElementById('lightbox-img');
  // Request a larger thumbnail for the lightbox
  const largeSrc = src.includes('?') ? src + '&size=large' : src + '?size=large';
  img.src = largeSrc;
  lightbox.classList.add('active');
  document.body.style.overflow = 'hidden';
}

function closeLightbox() {
  const lightbox = document.getElementById('image-lightbox');
  lightbox.classList.remove('active');
  document.getElementById('lightbox-img').src = '';
  document.body.style.overflow = '';
}

async function copyCaptionText() {
  const text = document.getElementById('caption-modal-text').textContent;
  try {
    await navigator.clipboard.writeText(text);
    showToast('הטקסט הועתק', 'success');
  } catch (e) {
    showToast('לא ניתן להעתיק', 'error');
  }
}

// ─── File ID Toggle ──────────────────────────────────────────
function toggleFileIds() {
  showFileIds = !showFileIds;
  const arrow = document.getElementById('file-id-toggle-arrow');
  if (arrow) arrow.innerHTML = showFileIds ? '&#9660;' : '&#9664;';
  renderPosts();
}

// ═══════════════════════════════════════════════════════════════
//  Drive Browser
// ═══════════════════════════════════════════════════════════════

function openDriveBrowser() {
  if (!config.driveFolderId) {
    showToast('לא הוגדרה תיקיית Drive. יש להגדיר GOOGLE_DRIVE_FOLDER_ID.', 'error');
    return;
  }

  selectedDriveFile = null;
  selectedDriveFiles = [];
  driveStack = [{ folderId: config.driveFolderId, name: 'תיקייה ראשית' }];
  _updateDriveConfirmBtn();
  openModal('drive-modal');
  loadDriveFolder(config.driveFolderId);
}

function closeDriveBrowser() {
  closeModal('drive-modal');
}

async function loadDriveFolder(folderId) {
  const browser = document.getElementById('drive-browser');
  const loading = document.getElementById('drive-loading');
  const empty = document.getElementById('drive-empty');

  browser.innerHTML = '';
  showElement('drive-loading');
  hideElement('drive-empty');

  try {
    const resp = await fetch(`/api/drive/files?folder_id=${encodeURIComponent(folderId)}`);
    const data = await resp.json();

    if (data.error) {
      showToast(data.error, 'error');
      return;
    }

    hideElement('drive-loading');
    const files = data.files || [];

    if (files.length === 0) {
      showElement('drive-empty');
      return;
    }

    // Separate folders and files
    const folders = files.filter(f => f.mimeType === 'application/vnd.google-apps.folder');
    const mediaFiles = files.filter(f => f.mimeType !== 'application/vnd.google-apps.folder');

    // Render folders first
    folders.forEach(f => {
      const el = document.createElement('div');
      el.className = 'drive-file drive-folder';
      el.dataset.folderId = f.id;
      el.dataset.folderName = f.name;
      el.innerHTML = `
        <div class="drive-file-icon">&#128193;</div>
        <div class="drive-file-name" title="${escapeHtml(f.name)}">${escapeHtml(f.name)}</div>
      `;
      el.addEventListener('dblclick', () => navigateDriveFolder(f.id, f.name));
      browser.appendChild(el);
    });

    // Render files
    mediaFiles.forEach(f => {
      const el = document.createElement('div');
      el.className = 'drive-file';
      el.dataset.fileId = f.id;
      el.dataset.fileName = f.name;
      const icon = getFileIcon(f.mimeType);
      const thumb = f.thumbnailLink
        ? `<img src="${escapeHtml(f.thumbnailLink)}" alt="${escapeHtml(f.name)}" loading="lazy">`
        : icon;
      el.innerHTML = `
        <div class="drive-file-icon">${thumb}</div>
        <div class="drive-file-name" title="${escapeHtml(f.name)}">${escapeHtml(f.name)}</div>
      `;
      el.addEventListener('click', () => selectDriveFile(el, f.id, f.name));
      browser.appendChild(el);
    });

    renderDriveBreadcrumb();

  } catch (e) {
    hideElement('drive-loading');
    showToast('שגיאה בטעינת קבצים מ-Drive', 'error');
    console.error(e);
  }
}

function navigateDriveFolder(folderId, name) {
  driveStack.push({ folderId, name });
  selectedDriveFile = null;
  selectedDriveFiles = [];
  _updateDriveConfirmBtn();
  loadDriveFolder(folderId);
}

function navigateDriveBreadcrumb(index) {
  driveStack = driveStack.slice(0, index + 1);
  selectedDriveFile = null;
  selectedDriveFiles = [];
  _updateDriveConfirmBtn();
  loadDriveFolder(driveStack[index].folderId);
}

function renderDriveBreadcrumb() {
  const el = document.getElementById('drive-breadcrumb');
  el.innerHTML = driveStack.map((item, i) => {
    const isLast = i === driveStack.length - 1;
    const link = isLast
      ? `<span style="color:var(--color-text-primary)">${escapeHtml(item.name)}</span>`
      : `<span class="drive-breadcrumb-item" onclick="navigateDriveBreadcrumb(${i})">${escapeHtml(item.name)}</span>`;
    const sep = i < driveStack.length - 1 ? '<span class="drive-breadcrumb-separator">/</span>' : '';
    return link + sep;
  }).join('');
}

function _isMultiSelectAllowed() {
  const channels = getSelectedChannels();
  // Multi-file carousel only supported for IG-only
  return channels.length === 1 && channels[0] === 'IG';
}

function selectDriveFile(el, fileId, fileName) {
  const multiAllowed = _isMultiSelectAllowed();

  const idx = selectedDriveFiles.findIndex(f => f.id === fileId);
  if (idx !== -1) {
    // Deselect
    selectedDriveFiles.splice(idx, 1);
    el.classList.remove('selected');
  } else {
    if (!multiAllowed && selectedDriveFiles.length >= 1) {
      // FB only — single select: replace previous selection
      document.querySelectorAll('.drive-file.selected').forEach(e => e.classList.remove('selected'));
      selectedDriveFiles = [];
    }
    if (selectedDriveFiles.length >= 10) {
      showToast('ניתן לבחור עד 10 קבצים לקרוסלה', 'error');
      return;
    }
    selectedDriveFiles.push({ id: fileId, name: fileName });
    el.classList.add('selected');
  }
  // Keep backward-compat for single file
  selectedDriveFile = selectedDriveFiles.length === 1 ? selectedDriveFiles[0] : null;
  _updateDriveConfirmBtn();
}

function _updateDriveConfirmBtn() {
  const btn = document.getElementById('btn-confirm-drive');
  const count = selectedDriveFiles.length;
  btn.disabled = count === 0;
  if (count > 1) {
    btn.textContent = `בחירת ${count} קבצים`;
  } else {
    btn.textContent = 'בחירה';
  }
}

function confirmDriveSelection() {
  if (selectedDriveFiles.length === 0) return;

  const ids = selectedDriveFiles.map(f => f.id).join(',');
  const names = selectedDriveFiles.map(f => f.name);
  const displayName = names.length === 1
    ? names[0]
    : `${names.length} קבצים: ${names.join(', ')}`;

  document.getElementById('form-drive-file-id').value = ids;
  document.getElementById('selected-file-name').textContent = displayName;
  showElement('drive-file-display');
  hideElement('form-drive-file-id-manual');

  closeDriveBrowser();
}

function clearDriveFile() {
  document.getElementById('form-drive-file-id').value = '';
  document.getElementById('form-drive-file-id-manual').value = '';
  selectedDriveFiles = [];
  selectedDriveFile = null;
  hideElement('drive-file-display');
}

function toggleManualFileId() {
  const el = document.getElementById('form-drive-file-id-manual');
  el.classList.toggle('hidden');
  if (!el.classList.contains('hidden')) {
    el.focus();
  }
}

// ─── Character Counter ──────────────────────────────────────
function updateCharCounter(type) {
  const inputId = type === 'general' ? 'form-caption' : `form-caption-${type}`;
  const textarea = document.getElementById(inputId);
  const counter = document.getElementById(`char-counter-${type}`);
  const countSpan = document.getElementById(`char-count-${type}`);
  if (!textarea || !counter || !countSpan) return;

  const len = textarea.value.length;
  const limit = CHAR_LIMITS[type];
  countSpan.textContent = len.toLocaleString();

  if (!limit) {
    // No limit for general caption
    counter.classList.remove('over-limit', 'near-limit');
    return;
  }

  if (len > limit) {
    counter.classList.add('over-limit');
    counter.classList.remove('near-limit');
  } else if (len > limit * 0.9) {
    counter.classList.remove('over-limit');
    counter.classList.add('near-limit');
  } else {
    counter.classList.remove('over-limit', 'near-limit');
  }
}

// ═══════════════════════════════════════════════════════════════
//  Calendar
// ═══════════════════════════════════════════════════════════════

const HEBREW_MONTHS = [
  'ינואר', 'פברואר', 'מרץ', 'אפריל', 'מאי', 'יוני',
  'יולי', 'אוגוסט', 'ספטמבר', 'אוקטובר', 'נובמבר', 'דצמבר'
];

const HEBREW_DAYS = ['א׳', 'ב׳', 'ג׳', 'ד׳', 'ה׳', 'ו׳', 'ש׳'];

function renderCalendar() {
  const grid = document.getElementById('calendar-grid');
  const year = calendarDate.getFullYear();
  const month = calendarDate.getMonth();

  document.getElementById('calendar-month-title').textContent =
    `${HEBREW_MONTHS[month]} ${year}`;

  // Day headers (Sunday first for Hebrew calendar)
  let html = HEBREW_DAYS.map(d =>
    `<div class="calendar-day-header">${d}</div>`
  ).join('');

  // First day of month
  const firstDay = new Date(year, month, 1);
  const startDay = firstDay.getDay(); // 0=Sunday

  // Days in month
  const daysInMonth = new Date(year, month + 1, 0).getDate();

  // Previous month padding
  const prevMonthDays = new Date(year, month, 0).getDate();
  for (let i = startDay - 1; i >= 0; i--) {
    html += `<div class="calendar-day other-month">
      <div class="calendar-day-number">${prevMonthDays - i}</div>
    </div>`;
  }

  // Current month days
  const today = new Date();
  for (let day = 1; day <= daysInMonth; day++) {
    const isToday = day === today.getDate() && month === today.getMonth() && year === today.getFullYear();

    // Find posts for this day
    const dayPosts = posts.filter(p => {
      if (!p.publish_at) return false;
      const pDate = parseDate(p.publish_at);
      if (!pDate) return false;
      return pDate.getFullYear() === year &&
             pDate.getMonth() === month &&
             pDate.getDate() === day;
    });

    const eventsHtml = dayPosts.slice(0, 3).map(p => {
      const status = (p.status || '').toLowerCase().replace('_', '-').replace(/[^a-z0-9-]/g, '');
      const net = escapeHtml(p.network || '');
      const time = p.publish_at ? formatTime(p.publish_at) : '';
      return `<div class="calendar-event status-${status}" title="${escapeHtml(p.caption_ig || p.caption_fb || '')}">${time} ${net}</div>`;
    }).join('');

    const moreHtml = dayPosts.length > 3
      ? `<div class="calendar-event" style="color:var(--color-text-muted)">+${dayPosts.length - 3} עוד</div>`
      : '';

    const dateStr = `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    html += `<div class="calendar-day clickable${isToday ? ' today' : ''}" onclick="openCreateModalWithDate('${dateStr}')">
      <div class="calendar-day-number">${day}</div>
      ${eventsHtml}${moreHtml}
    </div>`;
  }

  // Next month padding
  const totalCells = startDay + daysInMonth;
  const remaining = totalCells % 7 === 0 ? 0 : 7 - (totalCells % 7);
  for (let i = 1; i <= remaining; i++) {
    html += `<div class="calendar-day other-month">
      <div class="calendar-day-number">${i}</div>
    </div>`;
  }

  grid.innerHTML = html;
}

function calendarPrev() {
  calendarDate.setMonth(calendarDate.getMonth() - 1);
  renderCalendar();
}

function calendarNext() {
  calendarDate.setMonth(calendarDate.getMonth() + 1);
  renderCalendar();
}

function calendarToday() {
  calendarDate = new Date();
  renderCalendar();
}

function openCreateModalWithDate(dateStr) {
  loadGbpLocations('');
  resetPostForm({ title: 'פוסט חדש', publishAt: `${dateStr}T12:00` });
}

// ═══════════════════════════════════════════════════════════════
//  View Switching
// ═══════════════════════════════════════════════════════════════

function switchView(view) {
  currentView = view;

  // Toggle nav active
  document.querySelectorAll('.nav-item').forEach(el => {
    el.classList.toggle('active', el.dataset.view === view);
  });

  // Toggle views
  document.getElementById('view-posts').classList.toggle('hidden', view !== 'posts');
  document.getElementById('view-calendar').classList.toggle('hidden', view !== 'calendar');

  if (view === 'calendar') {
    renderCalendar();
  }
}

// ─── Sidebar Toggle ──────────────────────────────────────────
function toggleSidebar() {
  document.getElementById('sidebar').classList.toggle('collapsed');
}

function toggleFullscreen() {
  const layout = document.querySelector('.app-layout');
  const btn = document.getElementById('fullscreen-toggle');
  const isFullscreen = layout.classList.toggle('fullscreen');
  btn.innerHTML = isFullscreen ? '&#10005;' : '&#9724;';
  btn.title = isFullscreen ? 'יציאה ממסך מלא' : 'מסך מלא';
}

// ═══════════════════════════════════════════════════════════════
//  UI Helpers
// ═══════════════════════════════════════════════════════════════

function openModal(id) {
  document.getElementById(id).classList.add('active');
  document.body.style.overflow = 'hidden';
}

function closeModal(id) {
  document.getElementById(id).classList.remove('active');
  document.body.style.overflow = '';
}

// Close modal on backdrop click (except post-modal to prevent accidental data loss)
document.addEventListener('click', (e) => {
  if (e.target.classList.contains('modal-backdrop') && e.target.classList.contains('active')) {
    if (e.target.id === 'post-modal') return;
    e.target.classList.remove('active');
    document.body.style.overflow = '';
  }
});

// Close modal on Escape key
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    // Close lightbox if open
    const lightbox = document.getElementById('image-lightbox');
    if (lightbox && lightbox.classList.contains('active')) {
      closeLightbox();
      return;
    }
    let closedAny = false;
    document.querySelectorAll('.modal-backdrop.active').forEach(el => {
      if (el.id === 'post-modal') return;
      el.classList.remove('active');
      closedAny = true;
    });
    if (closedAny) document.body.style.overflow = '';
  }
});

function showElement(id) {
  document.getElementById(id).classList.remove('hidden');
}

function hideElement(id) {
  document.getElementById(id).classList.add('hidden');
}

// ─── Toast Notifications ─────────────────────────────────────
function showToast(message, type = 'info') {
  const container = document.getElementById('toast-container');
  const toast = document.createElement('div');
  toast.className = `toast toast-${type}`;
  toast.innerHTML = `<span>${escapeHtml(message)}</span>`;
  container.appendChild(toast);

  setTimeout(() => {
    toast.classList.add('removing');
    setTimeout(() => toast.remove(), 200);
  }, 4000);
}

// ─── Formatters ──────────────────────────────────────────────
function statusBadge(status) {
  const map = {
    'DRAFT': { class: 'badge-draft', label: 'טיוטה' },
    'READY': { class: 'badge-ready', label: 'ממתין' },
    'PROCESSING': { class: 'badge-in-progress', label: 'בתהליך' },
    'POSTED': { class: 'badge-posted', label: 'פורסם' },
    'PARTIAL': { class: 'badge-partial', label: 'חלקי' },
    'ERROR': { class: 'badge-error', label: 'שגיאה' },
  };
  const info = map[status] || { class: '', label: escapeHtml(status) || '-' };
  return `<span class="badge ${escapeHtml(info.class)}"><span class="badge-dot"></span>${info.label}</span>`;
}

function networkLabel(network) {
  const map = {
    'IG': 'IG',
    'FB': 'FB',
    'GBP': 'GBP',
    'LI': 'LI',
    'IG+FB': 'IG+FB',
    'IG+GBP': 'IG+GBP',
    'IG+LI': 'IG+LI',
    'FB+GBP': 'FB+GBP',
    'FB+LI': 'FB+LI',
    'GBP+LI': 'GBP+LI',
    'IG+FB+GBP': 'IG+FB+GBP',
    'IG+FB+LI': 'IG+FB+LI',
    'IG+GBP+LI': 'IG+GBP+LI',
    'FB+GBP+LI': 'FB+GBP+LI',
    'IG+FB+GBP+LI': 'IG+FB+GBP+LI',
    'ALL': 'הכל',
  };
  return map[network] || escapeHtml(network) || '-';
}

function postTypeLabel(type) {
  const map = {
    'FEED': 'פיד',
    'REELS': 'ריל',
    'STANDARD': 'עדכון',
  };
  return map[type] || escapeHtml(type) || '-';
}

// ─── Per-channel Results Display ─────────────────────────────
function channelResultsHtml(post) {
  const status = (post.status || '').toUpperCase();
  if (!status || status === 'DRAFT' || status === 'READY' || status === 'PROCESSING') return '';

  const published = (post.published_channels || '').split(',').map(s => s.trim()).filter(Boolean);
  const failed = (post.failed_channels || '').split(',').map(s => s.trim()).filter(Boolean);

  if (!published.length && !failed.length) {
    // Legacy post without per-channel data — show overall status
    if (status === 'POSTED') return '<span class="channel-badge channel-ok">&#10003;</span>';
    if (status === 'ERROR') return '<span class="channel-badge channel-err">&#10007;</span>';
    return '';
  }

  let html = '<div class="channel-results">';
  for (const ch of published) {
    html += `<span class="channel-badge channel-ok" title="${escapeHtml(ch)} — הצליח">${escapeHtml(ch)} &#10003;</span>`;
  }
  for (const ch of failed) {
    html += `<span class="channel-badge channel-err" title="${escapeHtml(ch)} — נכשל">${escapeHtml(ch)} &#10007;</span>`;
  }
  html += '</div>';
  return html;
}

// ─── Friendly Error Messages ─────────────────────────────────
const ERROR_CODE_LABELS = {
  'timeout': 'תקלת חיבור (timeout)',
  'rate_limit': 'חריגת מכסה — נסו שוב מאוחר יותר',
  'quota_exceeded': 'חריגת מכסה — נסו שוב מאוחר יותר',
  'http_429': 'חריגת מכסה — נסו שוב מאוחר יותר',
  'http_500': 'שגיאת שרת בפלטפורמה',
  'http_502': 'שגיאת שרת בפלטפורמה',
  'http_503': 'שגיאת שרת בפלטפורמה — נסו שוב',
  'http_504': 'תקלת חיבור לפלטפורמה',
  'invalid_caption': 'בעיה בטקסט הפוסט',
  'missing_location_id': 'חסר מיקום Google Business',
  'insufficient_permissions': 'אין הרשאות מתאימות',
  'unhandled_exception': 'שגיאה לא צפויה',
  'unexpected_error': 'שגיאה לא צפויה',
  'api_error': 'שגיאת API',
};

function friendlyError(errorStr) {
  if (!errorStr) return null;
  // Try to match known error codes from the result string
  for (const [code, label] of Object.entries(ERROR_CODE_LABELS)) {
    if (errorStr.includes(code)) return label;
  }
  // No known code found — return null (caller handles display)
  return null;
}

// ─── Retry Functions ─────────────────────────────────────────
async function retryAllFailed(rowNumber) {
  if (!confirm('לנסות שוב את כל הערוצים שנכשלו?')) return;
  await _doRetry(rowNumber, null);
}

async function _doRetry(rowNumber, channels) {
  try {
    const body = channels ? { channels } : {};
    const resp = await fetch(`/api/posts/${rowNumber}/retry`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const result = await resp.json();
    if (result.error) {
      showToast(result.error, 'error');
      return;
    }
    const retried = (result.retry_channels || []).join(', ');
    showToast(`Retry הופעל עבור ${retried}`, 'success');
    await loadPosts();
  } catch (e) {
    showToast('שגיאה בהפעלת retry', 'error');
    console.error(e);
  }
}

/**
 * Parse a date string safely across all browsers.
 * Safari requires ISO 8601 format (T separator), so we normalize
 * "YYYY-MM-DD HH:MM" to "YYYY-MM-DDTHH:MM" before parsing.
 */
function parseDate(str) {
  if (!str) return null;
  // Replace first space between date and time with T for ISO 8601 compat
  const normalized = str.replace(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})/, '$1T$2');
  const dt = new Date(normalized);
  return isNaN(dt) ? null : dt;
}

function formatDateTime(str) {
  if (!str) return '-';
  const dt = parseDate(str);
  if (!dt) return str;
  const pad = n => String(n).padStart(2, '0');
  return `${pad(dt.getDate())}/${pad(dt.getMonth() + 1)}/${dt.getFullYear()} ${pad(dt.getHours())}:${pad(dt.getMinutes())}`;
}

function formatTime(str) {
  if (!str) return '';
  const dt = parseDate(str);
  if (!dt) return '';
  const pad = n => String(n).padStart(2, '0');
  return `${pad(dt.getHours())}:${pad(dt.getMinutes())}`;
}

function truncate(str, max) {
  if (!str) return '<span style="color:var(--color-text-muted)">-</span>';
  return str.length > max ? escapeHtml(str.substring(0, max)) + '...' : escapeHtml(str);
}

function escapeHtml(str) {
  if (!str) return '';
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function getFileIcon(mimeType) {
  if (!mimeType) return '&#128196;';
  if (mimeType.startsWith('image/')) return '&#128247;';
  if (mimeType.startsWith('video/')) return '&#127909;';
  if (mimeType === 'application/vnd.google-apps.folder') return '&#128193;';
  return '&#128196;';
}

// ─── Scroll to Top Button ────────────────────────────────────
(function() {
  const btn = document.getElementById('scroll-top-btn');
  if (!btn) return;
  window.addEventListener('scroll', function() {
    btn.classList.toggle('visible', window.scrollY > 300);
  }, { passive: true });
})();

// ═══════════════════════════════════════════════════════════════
//  Real-time Status Polling
// ═══════════════════════════════════════════════════════════════

/**
 * Build a snapshot of { postId → status } from the current posts array.
 */
function buildStatusMap(postsArray) {
  const map = {};
  for (const p of postsArray) {
    if (p.id) map[p.id] = (p.status || '').toUpperCase();
  }
  return map;
}

/**
 * Start polling /api/posts/status every POLL_INTERVAL ms.
 * Automatically pauses when the tab is hidden (Page Visibility API).
 */
function startStatusPolling() {
  // Capture initial snapshot from already-loaded posts
  lastStatusMap = buildStatusMap(posts);

  // Visibility-aware polling
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      stopStatusPolling();
    } else {
      // When tab becomes visible again, poll immediately then resume interval
      pollStatus();
      schedulePoll();
    }
  });

  schedulePoll();
}

function schedulePoll() {
  stopStatusPolling();
  pollTimer = setInterval(pollStatus, POLL_INTERVAL);
}

function stopStatusPolling() {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

/**
 * Lightweight poll: fetch only IDs + statuses, detect changes,
 * then trigger a full reload + highlight when something changed.
 */
async function pollStatus() {
  if (pollInFlight) return; // prevent concurrent polls
  pollInFlight = true;

  try {
    const resp = await fetch('/api/posts/status');
    if (!resp.ok) return;

    const data = await resp.json();
    if (data.error || !data.statuses) return;

    // Build new status map from poll response
    const newMap = {};
    for (const s of data.statuses) {
      if (s.id) newMap[s.id] = (s.status || '').toUpperCase();
    }

    // Detect changed or newly added post IDs
    const changedIds = new Set();
    for (const [id, newStatus] of Object.entries(newMap)) {
      const oldStatus = lastStatusMap[id];
      if (oldStatus === undefined) {
        changedIds.add(id); // new post
      } else if (oldStatus !== newStatus) {
        changedIds.add(id); // status changed
      }
    }

    if (changedIds.size > 0) {
      // Silent reload — no spinner/flash, just update data in place
      await loadPosts(true);

      // Highlight changed rows/cards
      highlightChangedPosts(changedIds);
    } else {
      // No changes — safe to update snapshot from poll data
      lastStatusMap = newMap;
    }

  } catch (e) {
    // Silent fail — network hiccup, will retry next interval
    console.debug('Status poll failed:', e);
  } finally {
    pollInFlight = false;
  }
}

/**
 * Blink the status badge 3 times on rows/cards whose status changed.
 */
function highlightChangedPosts(changedIds) {
  // Desktop table rows — first cell contains the post ID
  document.querySelectorAll('#posts-tbody tr').forEach(tr => {
    const firstCell = tr.querySelector('td');
    if (firstCell && changedIds.has(firstCell.textContent.trim())) {
      tr.classList.add('status-changed');
      const badge = tr.querySelector('.badge');
      if (badge) {
        badge.addEventListener('animationend', () => tr.classList.remove('status-changed'), { once: true });
      }
    }
  });

  // Mobile cards — look for the ID span
  document.querySelectorAll('.post-card').forEach(card => {
    const idSpan = card.querySelector('.post-card-value');
    if (idSpan) {
      const idText = idSpan.textContent.replace('#', '').trim();
      if (changedIds.has(idText)) {
        card.classList.add('status-changed');
        const badge = card.querySelector('.badge');
        if (badge) {
          badge.addEventListener('animationend', () => card.classList.remove('status-changed'), { once: true });
        }
      }
    }
  });
}
