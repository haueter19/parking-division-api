/**
 * Cash Variance Entry JavaScript
 * Handles form submission, data loading, and editing
 */

const token = localStorage.getItem('access_token');
if (!token) {
    window.location.href = '/';
}

// Global data cache
let metadata = {
    facilities: [],
    devices: [],
    bag_types: []
};
let allEntries = [];

// ============= Initialization =============
document.addEventListener('DOMContentLoaded', async () => {
    await loadUserInfo();
    await loadMetadata();
    await loadEntries();
    setupFormHandlers();
});

// ============= User Info =============
async function loadUserInfo() {
    try {
        const response = await fetch('/api/v1/auth/me', {
            headers: { 'Authorization': `Bearer ${token}` }
        });

        if (response.ok) {
            const user = await response.json();
            document.getElementById('userName').textContent = user.full_name || user.username;
        } else {
            logout();
        }
    } catch (error) {
        console.error('Error loading user info:', error);
    }
}

function logout() {
    localStorage.removeItem('access_token');
    window.location.href = '/';
}

// ============= Metadata Loading =============
async function loadMetadata() {
    try {
        const response = await fetch('/api/v1/cash-variance/metadata', {
            headers: { 'Authorization': `Bearer ${token}` }
        });

        if (response.ok) {
            metadata = await response.json();
            populateDropdowns();
        } else {
            showMessage('Failed to load form options', 'error');
        }
    } catch (error) {
        console.error('Error loading metadata:', error);
        showMessage('Error loading form options', 'error');
    }
}

function populateDropdowns() {
    // Populate location dropdowns (facilities)
    const locationOptions = '<option value="">Select location...</option>' +
        metadata.facilities.map(f =>
            `<option value="${f.facility_id}">${f.facility_name}</option>`
        ).join('');

    document.getElementById('locationSelect').innerHTML = locationOptions;
    document.getElementById('filterLocation').innerHTML = '<option value="">All Locations</option>' +
        metadata.facilities.map(f =>
            `<option value="${f.facility_id}">${f.facility_name}</option>`
        ).join('');

    // Populate device dropdown
    const deviceOptions = '<option value="">Select station...</option>' +
        metadata.devices.map(d =>
            `<option value="${d.device_id}">${d.device_terminal_id} (${d.device_type})</option>`
        ).join('');

    document.getElementById('deviceSelect').innerHTML = deviceOptions;
}

// ============= Entries Loading =============
async function loadEntries() {
    const loading = document.getElementById('entriesLoading');
    const table = document.getElementById('entriesTable');

    loading.classList.add('show');
    table.style.display = 'none';

    // Build query params from filters
    const params = new URLSearchParams();

    const startDate = document.getElementById('filterStartDate').value;
    const endDate = document.getElementById('filterEndDate').value;
    const cashierNumber = document.getElementById('filterCashierNumber').value;
    const facilityId = document.getElementById('filterLocation').value;

    if (startDate) params.append('start_date', startDate + 'T00:00:00');
    if (endDate) params.append('end_date', endDate + 'T23:59:59');
    if (cashierNumber) params.append('cashier_number', cashierNumber);
    if (facilityId) params.append('facility_id', facilityId);

    try {
        const response = await fetch(`/api/v1/cash-variance?${params.toString()}`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });

        if (response.ok) {
            allEntries = await response.json();
            renderEntries(allEntries);
        } else {
            showMessage('Failed to load entries', 'error');
        }
    } catch (error) {
        console.error('Error loading entries:', error);
        showMessage('Error loading entries', 'error');
    } finally {
        loading.classList.remove('show');
        table.style.display = 'table';
    }
}

function renderEntries(entries) {
    const tbody = document.getElementById('entriesTableBody');
    const countEl = document.getElementById('entryCount');

    countEl.textContent = `(${entries.length} entries)`;

    if (!entries || entries.length === 0) {
        tbody.innerHTML = '<tr><td colspan="13" style="text-align: center; color: #6b7280;">No entries found</td></tr>';
        return;
    }

    tbody.innerHTML = entries.map(entry => `
        <tr>
            <td>${formatDate(entry.date)}</td>
            <td><strong>${entry.cashier_number}</strong></td>
            <td>${entry.bag_number}</td>
            <td><span class="badge badge-${entry.bag_type}">${formatBagType(entry.bag_type)}</span></td>
            <td>${entry.location_name || '-'}</td>
            <td>${entry.device_terminal_id || '-'}</td>
            <td class="amount">$${formatAmount(entry.amount)}</td>
            <td>${entry.turnarounds}</td>
            <td>${entry.ftp_count}</td>
            <td class="amount">$${formatAmount(entry.coupons)}</td>
            <td>${entry.other_non_paying}</td>
            <td>${entry.created_by_name || '-'}</td>
            <td>
                <button class="btn-secondary action-btn" onclick="editEntry(${entry.id})">Edit</button>
            </td>
        </tr>
    `).join('');
}

// ============= Form Handlers =============
function setupFormHandlers() {
    // Create form
    document.getElementById('cashVarianceForm').addEventListener('submit', async (e) => {
        e.preventDefault();

        const formData = new FormData(e.target);
        const data = {
            date: formData.get('date'),
            cashier_number: formData.get('cashier_number'),
            bag_number: formData.get('bag_number'),
            bag_type: formData.get('bag_type'),
            location_id: formData.get('location_id') ? parseInt(formData.get('location_id')) : null,
            device_id: formData.get('device_id') ? parseInt(formData.get('device_id')) : null,
            amount: formData.get('amount') ? parseFloat(formData.get('amount')) : null,
            turnarounds: parseInt(formData.get('turnarounds')) || 0,
            ftp_count: parseInt(formData.get('ftp_count')) || 0,
            coupons: formData.get('coupons') ? parseFloat(formData.get('coupons')) : 0,
            other_non_paying: parseInt(formData.get('other_non_paying')) || 0
        };

        try {
            const response = await fetch('/api/v1/cash-variance', {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(data)
            });

            if (response.ok) {
                showMessage('Entry created successfully!', 'success');
                e.target.reset();
                await loadEntries();
            } else {
                const error = await response.json();
                showMessage(error.detail || 'Failed to create entry', 'error');
            }
        } catch (error) {
            showMessage('Error creating entry: ' + error.message, 'error');
        }
    });

    // Edit form
    document.getElementById('editForm').addEventListener('submit', async (e) => {
        e.preventDefault();

        const formData = new FormData(e.target);
        const entryId = formData.get('entry_id');

        const data = {
            date: formData.get('date'),
            cashier_number: formData.get('cashier_number'),
            bag_number: formData.get('bag_number'),
            bag_type: formData.get('bag_type'),
            location_id: formData.get('location_id') ? parseInt(formData.get('location_id')) : null,
            device_id: formData.get('device_id') ? parseInt(formData.get('device_id')) : null,
            amount: formData.get('amount') ? parseFloat(formData.get('amount')) : null,
            turnarounds: parseInt(formData.get('turnarounds')) || 0,
            ftp_count: parseInt(formData.get('ftp_count')) || 0,
            coupons: formData.get('coupons') ? parseFloat(formData.get('coupons')) : 0,
            other_non_paying: parseInt(formData.get('other_non_paying')) || 0
        };

        try {
            const response = await fetch(`/api/v1/cash-variance/${entryId}`, {
                method: 'PUT',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(data)
            });

            if (response.ok) {
                showMessage('Entry updated successfully!', 'success');
                closeEditModal();
                await loadEntries();
            } else {
                const error = await response.json();
                showMessage(error.detail || 'Failed to update entry', 'error');
            }
        } catch (error) {
            showMessage('Error updating entry: ' + error.message, 'error');
        }
    });
}

// ============= Filter Functions =============
function clearFilters() {
    document.getElementById('filterStartDate').value = '';
    document.getElementById('filterEndDate').value = '';
    document.getElementById('filterCashierNumber').value = '';
    document.getElementById('filterLocation').value = '';
    loadEntries();
}

// ============= Edit Modal =============
function editEntry(entryId) {
    const entry = allEntries.find(e => e.id === entryId);
    if (!entry) {
        showMessage('Entry not found', 'error');
        return;
    }

    // Build location options
    const locationOptions = '<option value="">Select location...</option>' +
        metadata.facilities.map(f =>
            `<option value="${f.facility_id}" ${entry.location_id === f.facility_id ? 'selected' : ''}>${f.facility_name}</option>`
        ).join('');

    // Build device options
    const deviceOptions = '<option value="">Select station...</option>' +
        metadata.devices.map(d =>
            `<option value="${d.device_id}" ${entry.device_id === d.device_id ? 'selected' : ''}>${d.device_terminal_id} (${d.device_type})</option>`
        ).join('');

    // Populate edit form
    const formFields = document.getElementById('editFormFields');
    formFields.innerHTML = `
        <input type="hidden" name="entry_id" value="${entry.id}">
        <div class="form-group">
            <label>Date <span class="required">*</span></label>
            <input type="datetime-local" name="date" required value="${formatDateTimeLocal(new Date(entry.date))}">
        </div>
        <div class="form-group">
            <label>Cashier Number <span class="required">*</span></label>
            <input type="text" name="cashier_number" required value="${entry.cashier_number}">
        </div>
        <div class="form-group">
            <label>Bag Number <span class="required">*</span></label>
            <input type="text" name="bag_number" required value="${entry.bag_number}">
        </div>
        <div class="form-group">
            <label>Bag Type <span class="required">*</span></label>
            <select name="bag_type" required>
                <option value="regular" ${entry.bag_type === 'regular' ? 'selected' : ''}>Regular</option>
                <option value="special_event" ${entry.bag_type === 'special_event' ? 'selected' : ''}>Special Event</option>
            </select>
        </div>
        <div class="form-group">
            <label>Location</label>
            <select name="location_id">${locationOptions}</select>
        </div>
        <div class="form-group">
            <label>Station/Device</label>
            <select name="device_id">${deviceOptions}</select>
        </div>
        <div class="form-group">
            <label>Amount ($)</label>
            <input type="number" name="amount" step="0.01" min="0" value="${entry.amount || ''}">
        </div>
        <div class="form-group">
            <label>Turnarounds</label>
            <input type="number" name="turnarounds" min="0" value="${entry.turnarounds}">
        </div>
        <div class="form-group">
            <label>FTP Count</label>
            <input type="number" name="ftp_count" min="0" value="${entry.ftp_count}">
        </div>
        <div class="form-group">
            <label>Coupons ($)</label>
            <input type="number" name="coupons" step="0.01" min="0" value="${entry.coupons}">
        </div>
        <div class="form-group">
            <label>Other Non-Paying</label>
            <input type="number" name="other_non_paying" min="0" value="${entry.other_non_paying}">
        </div>
    `;

    document.getElementById('editModal').classList.add('show');
}

function closeEditModal() {
    document.getElementById('editModal').classList.remove('show');
}

// ============= Utility Functions =============
function showMessage(text, type) {
    const message = document.getElementById('message');
    message.textContent = text;
    message.className = `message show ${type}`;

    setTimeout(() => {
        message.classList.remove('show');
    }, 5000);
}

function formatDate(dateString) {
    if (!dateString) return '-';
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function formatDateTimeLocal(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    return `${year}-${month}-${day}T${hours}:${minutes}`;
}

function formatAmount(amount) {
    if (amount === null || amount === undefined) return '0.00';
    return parseFloat(amount).toFixed(2);
}

function formatBagType(bagType) {
    if (bagType === 'special_event') return 'Special Event';
    return 'Regular';
}

function toggleFormSection(header) {
    const content = header.nextElementSibling;
    const icon = header.querySelector('.toggle-icon');

    if (content.classList.contains('expanded')) {
        content.classList.remove('expanded');
        icon.textContent = '+';
    } else {
        content.classList.add('expanded');
        icon.textContent = '-';
    }
}

// Close modal when clicking outside
window.addEventListener('click', (event) => {
    const modal = document.getElementById('editModal');
    if (event.target === modal) {
        closeEditModal();
    }
});
