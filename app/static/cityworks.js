/**
 * Cityworks Work Orders - Frontend Logic
 * Handles work order listing, filtering, sorting, and navigation
 */

// Authentication token
const token = localStorage.getItem('access_token');
if (!token) {
    window.location.href = '/';
}

// Global state
let allWorkOrders = [];
let filteredWorkOrders = [];
let filterOptions = {};
let sortColumn = 'initiate_date';
let sortDirection = 'desc';
let filtersVisible = true;

// ==================== Initialization ====================

document.addEventListener('DOMContentLoaded', async () => {
    await loadUserInfo();
    await loadFilterOptions();
    await loadWorkOrders();
    setupEventListeners();
});

// ==================== User Info ====================

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

// ==================== Filter Options ====================

async function loadFilterOptions() {
    try {
        const response = await fetch('/api/v1/cityworks/filter-options', {
            headers: { 'Authorization': `Bearer ${token}` }
        });

        if (response.ok) {
            filterOptions = await response.json();
            populateFilterDropdowns();
        }
    } catch (error) {
        console.error('Error loading filter options:', error);
    }
}

function populateFilterDropdowns() {
    // Populate Status dropdown
    const statusSelect = document.getElementById('filterStatus');
    if (filterOptions.statuses && filterOptions.statuses.length > 0) {
        filterOptions.statuses.forEach(status => {
            const option = document.createElement('option');
            option.value = status;
            option.textContent = status;
            statusSelect.appendChild(option);
        });
    }

    // Populate Submit To dropdown
    const submitToSelect = document.getElementById('filterSubmitTo');
    if (filterOptions.submit_to_options && filterOptions.submit_to_options.length > 0) {
        filterOptions.submit_to_options.forEach(item => {
            const option = document.createElement('option');
            option.value = item;
            option.textContent = item;
            submitToSelect.appendChild(option);
        });
    }

    // Populate Parent Template dropdown
    const parentTemplateSelect = document.getElementById('filterParentTemplate');
    if (filterOptions.parent_templates && filterOptions.parent_templates.length > 0) {
        filterOptions.parent_templates.forEach(template => {
            const option = document.createElement('option');
            option.value = template;
            option.textContent = template;
            parentTemplateSelect.appendChild(option);
        });
    }

    // Populate Requested By dropdown
    const requestedBySelect = document.getElementById('filterRequestedBy');
    if (filterOptions.requested_by_options && filterOptions.requested_by_options.length > 0) {
        filterOptions.requested_by_options.forEach(item => {
            const option = document.createElement('option');
            option.value = item;
            option.textContent = item;
            requestedBySelect.appendChild(option);
        });
    }
}

// ==================== Work Orders ====================

async function loadWorkOrders() {
    showLoading();

    try {
        const response = await fetch('/api/v1/cityworks/work-orders', {
            headers: { 'Authorization': `Bearer ${token}` }
        });

        if (!response.ok) {
            throw new Error('Failed to load work orders');
        }

        const data = await response.json();
        allWorkOrders = data.work_orders || [];
        filteredWorkOrders = [...allWorkOrders];

        updateStats();
        sortWorkOrders();
        renderWorkOrders();

    } catch (error) {
        console.error('Error loading work orders:', error);
        showError('Error loading work orders: ' + error.message);
    }
}

function renderWorkOrders() {
    const tbody = document.getElementById('workOrdersBody');

    if (filteredWorkOrders.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="8">
                    <div class="empty-state">
                        <div class="empty-state-icon">&#128269;</div>
                        <p>No work orders found matching your filters.</p>
                    </div>
                </td>
            </tr>
        `;
        return;
    }

    let html = '';
    filteredWorkOrders.forEach(wo => {
        html += `
            <tr onclick="openWorkOrder('${wo.work_order_id}')">
                <td>${wo.work_order_id || '--'}</td>
                <td>${truncateText(wo.description, 50) || '--'}</td>
                <td>${getStatusBadge(wo.status)}</td>
                <td>${wo.submit_to || '--'}</td>
                <td>${formatDate(wo.initiate_date)}</td>
                <td>${formatDate(wo.actual_start_date)}</td>
                <td>${truncateText(wo.parent_template, 30) || '--'}</td>
                <td>${wo.requested_by || '--'}</td>
            </tr>
        `;
    });

    tbody.innerHTML = html;
}

function openWorkOrder(workOrderId) {
    window.location.href = `/cityworks/detail?id=${workOrderId}`;
}

// ==================== Filtering ====================

function applyFilters() {
    const statusFilter = document.getElementById('filterStatus').value;
    const submitToFilter = document.getElementById('filterSubmitTo').value;
    const initiateDateStart = document.getElementById('filterInitiateDateStart').value;
    const initiateDateEnd = document.getElementById('filterInitiateDateEnd').value;
    const actualStartDateStart = document.getElementById('filterActualStartDateStart').value;
    const actualStartDateEnd = document.getElementById('filterActualStartDateEnd').value;
    const parentTemplateFilter = document.getElementById('filterParentTemplate').value;
    const requestedByFilter = document.getElementById('filterRequestedBy').value;

    filteredWorkOrders = allWorkOrders.filter(wo => {
        // Status filter
        if (statusFilter && wo.status !== statusFilter) {
            return false;
        }

        // Submit To filter
        if (submitToFilter && wo.submit_to !== submitToFilter) {
            return false;
        }

        // Parent Template filter
        if (parentTemplateFilter && wo.parent_template !== parentTemplateFilter) {
            return false;
        }

        // Requested By filter
        if (requestedByFilter && wo.requested_by !== requestedByFilter) {
            return false;
        }

        // Initiate Date range filter
        if (initiateDateStart && wo.initiate_date) {
            const woDate = new Date(wo.initiate_date);
            const filterDate = new Date(initiateDateStart);
            if (woDate < filterDate) return false;
        }
        if (initiateDateEnd && wo.initiate_date) {
            const woDate = new Date(wo.initiate_date);
            const filterDate = new Date(initiateDateEnd);
            filterDate.setHours(23, 59, 59, 999);
            if (woDate > filterDate) return false;
        }

        // Actual Start Date range filter
        if (actualStartDateStart && wo.actual_start_date) {
            const woDate = new Date(wo.actual_start_date);
            const filterDate = new Date(actualStartDateStart);
            if (woDate < filterDate) return false;
        }
        if (actualStartDateEnd && wo.actual_start_date) {
            const woDate = new Date(wo.actual_start_date);
            const filterDate = new Date(actualStartDateEnd);
            filterDate.setHours(23, 59, 59, 999);
            if (woDate > filterDate) return false;
        }

        return true;
    });

    updateStats();
    sortWorkOrders();
    renderWorkOrders();
}

function clearFilters() {
    document.getElementById('filterStatus').value = '';
    document.getElementById('filterSubmitTo').value = '';
    document.getElementById('filterInitiateDateStart').value = '';
    document.getElementById('filterInitiateDateEnd').value = '';
    document.getElementById('filterActualStartDateStart').value = '';
    document.getElementById('filterActualStartDateEnd').value = '';
    document.getElementById('filterParentTemplate').value = '';
    document.getElementById('filterRequestedBy').value = '';

    filteredWorkOrders = [...allWorkOrders];
    updateStats();
    sortWorkOrders();
    renderWorkOrders();
}

function toggleFilters() {
    const filtersContent = document.getElementById('filtersContent');
    const toggleBtn = document.getElementById('toggleFiltersBtn');

    filtersVisible = !filtersVisible;

    if (filtersVisible) {
        filtersContent.style.display = 'block';
        toggleBtn.textContent = 'Hide Filters';
    } else {
        filtersContent.style.display = 'none';
        toggleBtn.textContent = 'Show Filters';
    }
}

// ==================== Sorting ====================

function sortWorkOrders() {
    filteredWorkOrders.sort((a, b) => {
        let valA = a[sortColumn];
        let valB = b[sortColumn];

        // Handle null/undefined values
        if (valA === null || valA === undefined) valA = '';
        if (valB === null || valB === undefined) valB = '';

        // Handle date sorting
        if (sortColumn.includes('date')) {
            valA = valA ? new Date(valA).getTime() : 0;
            valB = valB ? new Date(valB).getTime() : 0;
        } else {
            // String comparison
            valA = String(valA).toLowerCase();
            valB = String(valB).toLowerCase();
        }

        if (valA < valB) return sortDirection === 'asc' ? -1 : 1;
        if (valA > valB) return sortDirection === 'asc' ? 1 : -1;
        return 0;
    });
}

function handleSort(column) {
    if (sortColumn === column) {
        // Toggle direction
        sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
    } else {
        // New column, default to ascending
        sortColumn = column;
        sortDirection = 'asc';
    }

    // Update UI to show sort state
    updateSortIndicators();

    sortWorkOrders();
    renderWorkOrders();
}

function updateSortIndicators() {
    // Remove sorted class from all headers
    document.querySelectorAll('th.sortable').forEach(th => {
        th.classList.remove('sorted');
        const icon = th.querySelector('.sort-icon');
        if (icon) icon.innerHTML = '&#8597;';
    });

    // Add sorted class and update icon for current column
    const currentHeader = document.querySelector(`th[data-sort="${sortColumn}"]`);
    if (currentHeader) {
        currentHeader.classList.add('sorted');
        const icon = currentHeader.querySelector('.sort-icon');
        if (icon) {
            icon.innerHTML = sortDirection === 'asc' ? '&#8593;' : '&#8595;';
        }
    }
}

// ==================== Event Listeners ====================

function setupEventListeners() {
    // Sortable headers
    document.querySelectorAll('th.sortable').forEach(th => {
        th.addEventListener('click', () => {
            const column = th.dataset.sort;
            handleSort(column);
        });
    });

    // Enter key on filter inputs
    document.querySelectorAll('.filter-group input, .filter-group select').forEach(el => {
        el.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                applyFilters();
            }
        });
    });
}

// ==================== Utility Functions ====================

function updateStats() {
    document.getElementById('totalCount').textContent = allWorkOrders.length;
    document.getElementById('filteredCount').textContent = filteredWorkOrders.length;
}

function showLoading() {
    document.getElementById('workOrdersBody').innerHTML = `
        <tr>
            <td colspan="8">
                <div class="loading">
                    <div class="loading-spinner"></div>
                    Loading work orders...
                </div>
            </td>
        </tr>
    `;
}

function showError(message) {
    const errorDiv = document.getElementById('errorMessage');
    errorDiv.textContent = message;
    errorDiv.style.display = 'block';

    document.getElementById('workOrdersBody').innerHTML = `
        <tr>
            <td colspan="8">
                <div class="empty-state">
                    <div class="empty-state-icon">&#9888;</div>
                    <p>Error loading work orders. Please try again.</p>
                </div>
            </td>
        </tr>
    `;
}

function getStatusBadge(status) {
    if (!status) return '<span class="status-badge">Unknown</span>';

    const statusLower = status.toLowerCase();
    let className = 'status-badge';

    if (statusLower.includes('open')) className += ' status-open';
    else if (statusLower.includes('complete')) className += ' status-in-progress';
    else if (statusLower.includes('pending')) className += ' status-pending';
    else if (statusLower.includes('closed') || statusLower.includes('complete')) className += ' status-closed';

    return `<span class="${className}">${status}</span>`;
}

function formatDate(dateStr) {
    if (!dateStr) return '--';
    try {
        return new Date(dateStr).toLocaleDateString('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric'
        });
    } catch {
        return dateStr;
    }
}

function truncateText(text, maxLength) {
    if (!text) return '';
    if (text.length <= maxLength) return text;
    return text.substring(0, maxLength) + '...';
}
