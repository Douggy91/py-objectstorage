const API_BASE = '/_s3/api/ui';
const S3_BASE = '/_s3'; // Root is S3

const state = {
    buckets: [],
    currentBucket: null,
    objects: {}, // grouped by key
    showVersions: false,
    token: null
};

async function init() {
    // Check for existing token
    const savedToken = localStorage.getItem('auth_token');
    if (savedToken) {
        state.token = savedToken;
        document.getElementById('loginOverlay').classList.add('hidden');
        await loadDashboard();
    } else {
        // Show login
        document.getElementById('loginOverlay').classList.remove('hidden');
    }

    // Login Handlers
    document.getElementById('loginBtn').addEventListener('click', doLogin);

    // Event Listeners
    document.getElementById('createBucketBtn').addEventListener('click', () => {
        showModal('Create Bucket', '<input type="text" id="newBucketName" placeholder="Bucket Name" style="width:100%; padding: 10px; background: #0f172a; border: 1px solid #334155; color: white; border-radius: 4px;">', async () => {
            const name = document.getElementById('newBucketName').value;
            if (name) {
                await authenticatedFetch(`${S3_BASE}/${name}`, { method: 'PUT' });
                await fetchBuckets();
                renderBuckets();
                hideModal();
            }
        });
    });

    document.getElementById('showVersionsToggle').addEventListener('change', (e) => {
        state.showVersions = e.target.checked;
        renderObjects();
    });

    document.getElementById("uploadBtn").addEventListener('click', () => {
        // Trigger file input
        const input = document.createElement('input');
        input.type = 'file';
        input.onchange = async (e) => {
            if (e.target.files.length > 0) {
                const file = e.target.files[0];
                await uploadFile(file);
            }
        };
        input.click();
    });
}

async function doLogin() {
    const user = document.getElementById('loginUsername').value;
    const pass = document.getElementById('loginPassword').value;

    try {
        const res = await fetch('/_s3/api/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username: user, password: pass })
        });

        if (res.ok) {
            const data = await res.json();
            state.token = data.token;
            localStorage.setItem('auth_token', data.token);
            document.getElementById('loginOverlay').classList.add('hidden');
            document.getElementById('loginError').style.display = 'none';
            await loadDashboard();
        } else {
            document.getElementById('loginError').style.display = 'block';
        }
    } catch (e) {
        console.error(e);
        document.getElementById('loginError').innerText = "Network Error";
        document.getElementById('loginError').style.display = 'block';
    }
}

async function authenticatedFetch(url, options = {}) {
    if (!options.headers) options.headers = {};
    options.headers['x-auth-token'] = state.token;

    const res = await fetch(url, options);
    console.log('Fetch:', url, res.status);
    if (res.status === 401) {
        // Logout
        state.token = null;
        localStorage.removeItem('auth_token');
        document.getElementById('loginOverlay').classList.remove('hidden');
        return null; // Stop
    }
    return res;
}

async function loadDashboard() {
    await fetchBuckets();
    renderBuckets();
}

async function fetchBuckets() {
    const res = await authenticatedFetch(`${API_BASE}/buckets`);
    if (!res) return;
    state.buckets = await res.json();
}

async function selectBucket(name) {
    state.currentBucket = name;
    renderBuckets(); // update active state
    document.getElementById('breadcrumb').innerText = name;
    document.getElementById('actionsBar').style.display = 'flex';

    const res = await authenticatedFetch(`${API_BASE}/${name}/objects`);
    if (!res) return;
    state.objects = await res.json();
    renderObjects();
}

function renderBuckets() {
    const list = document.getElementById('bucketList');
    list.innerHTML = '';
    state.buckets.forEach(b => {
        const li = document.createElement('li');
        li.className = `bucket-item ${state.currentBucket === b.name ? 'active' : ''}`;
        li.innerText = b.name;
        li.onclick = () => selectBucket(b.name);
        list.appendChild(li);
    });
}

function renderObjects() {
    const tbody = document.getElementById('objectList');
    tbody.innerHTML = '';

    const keys = Object.keys(state.objects).sort();

    keys.forEach(key => {
        const versions = state.objects[key];

        // If not showing versions, only show the one marked is_latest (even if delete marker)
        // If delete marker is latest, and !showVersions, usually we show nothing (deleted)

        versions.forEach(ver => {
            if (!state.showVersions && !ver.is_latest) return;
            if (!state.showVersions && ver.is_latest && ver.is_delete_marker) return;

            const tr = document.createElement('tr');

            // Format size
            const sizeStr = ver.is_delete_marker ? '-' : (ver.size < 1024 ? ver.size + ' B' : (ver.size / 1024).toFixed(1) + ' KB');

            // Status Badge
            let statusBadge = '';
            if (ver.is_delete_marker) statusBadge = '<span class="status-badge status-delete">Deleted</span>';
            else if (ver.is_latest) statusBadge = '<span class="status-badge status-latest">Latest</span>';
            else statusBadge = '<span class="status-badge status-history">v' + ver.version_id.substring(0, 6) + '...</span>';

            tr.innerHTML = `
                <td>${key}</td>
                <td><span style="font-family:monospace; color:var(--text-secondary);">${ver.version_id.substring(0, 8)}...</span></td>
                <td>${sizeStr}</td>
                <td>${new Date(ver.last_modified).toLocaleString()}</td>
                <td>${statusBadge}</td>
                <td>
                    ${!ver.is_delete_marker ? `<button class="action-btn" onclick="downloadObject('${key}', '${ver.version_id}')">Download</button>` : ''}
                    ${!ver.is_latest ? `<button class="action-btn action-rollback" onclick="rollbackObject('${key}', '${ver.version_id}')">Rollback</button>` : ''}
                    <button class="action-btn" onclick="deleteObject('${key}', '${ver.version_id}')" style="color:var(--danger); border-color:var(--danger)">Del</button>
                </td>
            `;
            tbody.appendChild(tr);
        });
    });
}

// --- Actions ---

async function uploadFile(file) {
    if (!state.currentBucket) return;
    const bucket = state.currentBucket;

    // Use raw S3 PUT
    await authenticatedFetch(`${S3_BASE}/${bucket}/${file.name}`, {
        method: 'PUT',
        body: file
    });

    selectBucket(bucket); // Refresh
}

function downloadObject(key, versionId) {
    // Determine URL
    let url = `${S3_BASE}/${state.currentBucket}/${key}`;
    if (versionId) url += `?versionId=${versionId}`;
    window.open(url, '_blank');
}

async function rollbackObject(key, versionId) {
    if (!confirm(`Are you sure you want to rollback to version ${versionId}? WARNING: All newer versions created after this version will be PERMANENTLY DELETED.`)) return;

    const res = await authenticatedFetch(`${API_BASE}/${state.currentBucket}/rollback?key=${encodeURIComponent(key)}&version_id=${versionId}`, {
        method: 'POST'
    });

    if (res && res.ok) {
        selectBucket(state.currentBucket);
    } else {
        alert("Rollback failed");
    }
}

async function deleteObject(key, versionId) {
    if (!confirm("Are you sure?")) return;

    let url = `${S3_BASE}/${state.currentBucket}/${key}`;
    if (versionId) url += `?versionId=${versionId}`;

    await fetch(url, { method: 'DELETE' });
    selectBucket(state.currentBucket);
}

// --- Modal System ---
function showModal(title, content, onConfirm) {
    document.getElementById('modalTitle').innerText = title;
    document.getElementById('modalContent').innerHTML = content;
    document.getElementById('modalOverlay').classList.remove('hidden');

    const confirmBtn = document.getElementById('modalConfirm');
    // Remove old listeners
    const newBtn = confirmBtn.cloneNode(true);
    confirmBtn.parentNode.replaceChild(newBtn, confirmBtn);

    newBtn.addEventListener('click', onConfirm);

    document.getElementById('modalCancel').onclick = hideModal;
}

function hideModal() {
    document.getElementById('modalOverlay').classList.add('hidden');
}

// Start
init();
