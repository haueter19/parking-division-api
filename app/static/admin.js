/**
 * Admin Configuration JavaScript
 * Handles all admin page interactions and API calls
 */

const token = localStorage.getItem('access_token');
if (!token) {
    window.location.href = '/';
}

// ============= Tab Management =============

function showTab(tabName) {
    // Hide all tabs
    document.querySelectorAll('.tab-content').forEach(tab => {
        tab.classList.remove('active');
    });
    
    // Remove active from all tab buttons
    document.querySelectorAll('.tab').forEach(btn => {
        btn.classList.remove('active');
    });
    
    // Show selected tab
    document.getElementById(`${tabName}-tab`).classList.add('active');
    event.target.classList.add('active');
    
    // Load data for the tab
    switch(tabName) {
        case 'devices':
            loadDevices();
            break;
        case 'assignments':
            loadAssignments();
            loadDevicesForDropdown();
            loadFacilitiesForDropdown();
            break;
        case 'settlement':
            loadSettlementSystems();
            break;
        case 'payment':
            loadPaymentMethods();
            break;
    }
}

// Admin metadata cache (devices, locations, facilities, device_types)
let adminMetadata = null;

async function loadAdminMetadata() {
    if (adminMetadata) return adminMetadata;
    try {
        const resp = await fetch('/api/v1/admin/metadata', { headers: { 'Authorization': `Bearer ${token}` } });
        if (resp.ok) {
            adminMetadata = await resp.json();
            return adminMetadata;
        }
    } catch (err) {
        console.error('Error loading admin metadata', err);
    }
    // Fallback to empty structures
    adminMetadata = { devices: [], locations: [], facilities: [], device_types: [] };
    return adminMetadata;
}

// ============= User Info =============

async function loadUserInfo() {
    try {
        const response = await fetch('/api/v1/auth/me', {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (response.ok) {
            const user = await response.json();
            document.getElementById('userName').textContent = user.full_name || user.username;
            
            // Check if user is admin
            if (user.role !== 'admin') {
                showMessage('Access denied. Admin privileges required.', 'error');
                setTimeout(() => window.location.href = '/upload', 2000);
            }
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

// ============= Message Display =============

function showMessage(text, type) {
    const message = document.getElementById('message');
    message.textContent = text;
    message.className = `message show ${type}`;
    
    setTimeout(() => {
        message.classList.remove('show');
    }, 5000);
}

// ============= Device Management =============

document.getElementById('deviceForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const formData = new FormData(e.target);
    const data = {
        device_terminal_id: formData.get('device_terminal_id'),
        device_type: formData.get('device_type'),
        supports_cash: formData.get('supports_cash') === 'on',
        supports_card: formData.get('supports_card') === 'on',
        supports_mobile: formData.get('supports_mobile') === 'on',
        cwAssetID: formData.get('cwAssetID') || null,
        SerialNumber: formData.get('SerialNumber') || null,
        Brand: formData.get('Brand') || null,
        Model: formData.get('Model') || null
    };
    
    try {
        const response = await fetch('/api/v1/admin/devices', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });
        
        if (response.ok) {
            showMessage('Device created successfully!', 'success');
            e.target.reset();
            loadDevices();
        } else {
            const error = await response.json();
            showMessage(error.detail || 'Failed to create device', 'error');
        }
    } catch (error) {
        showMessage('Error creating device', 'error');
    }
});

async function loadDevices() {
    const loading = document.getElementById('devicesLoading');
    const table = document.getElementById('devicesTable');
    const tbody = document.getElementById('devicesTableBody');
    
    loading.classList.add('show');
    table.style.display = 'none';
    
    try {
        const response = await fetch('/api/v1/admin/devices', {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (response.ok) {
            const devices = await response.json();
            
            tbody.innerHTML = devices.map(device => `
                <tr>
                    <td>${device.device_id}</td>
                    <td><strong>${device.device_terminal_id}</strong></td>
                    <td>${device.device_type}</td>
                    <td>
                        ${device.supports_cash ? '<span class="badge badge-success">Cash</span>' : ''}
                        ${device.supports_card ? '<span class="badge badge-info">Card</span>' : ''}
                        ${device.supports_mobile ? '<span class="badge badge-warning">Mobile</span>' : ''}
                    </td>
                    <td>${device.Brand || ''} ${device.Model || ''}</td>
                    <td>
                        <button class="btn-secondary action-btn" onclick="viewDeviceDetails(${device.device_id})">View</button>
                    </td>
                </tr>
            `).join('');
            
            table.style.display = 'table';
        }
    } catch (error) {
        showMessage('Error loading devices', 'error');
    } finally {
        loading.classList.remove('show');
    }
}

function viewDeviceDetails(deviceId) {
    // Could implement a detail modal here
    alert(`View details for device ${deviceId}`);
}

// ============= Device Assignment Management =============

// Filter state for assignments
const assignmentFilters = {
    device_id: null,
    location_id: null,
    device_type: null,
    facility_id: null,
    active_only: true
};

function applyAssignmentFilters() {
    const deviceId = document.getElementById('filterDeviceSelect').value;
    const locationId = document.getElementById('filterLocationSelect').value;
    const deviceType = document.getElementById('filterDeviceTypeSelect') ? document.getElementById('filterDeviceTypeSelect').value : '';
    const facilityId = document.getElementById('filterFacilitySelect') ? document.getElementById('filterFacilitySelect').value : '';
    const status = document.getElementById('filterStatusSelect').value;
    
    assignmentFilters.device_id = deviceId ? parseInt(deviceId) : null;
    assignmentFilters.location_id = locationId ? parseInt(locationId) : null;
    assignmentFilters.device_type = deviceType || null;
    assignmentFilters.facility_id = facilityId ? parseInt(facilityId) : null;
    
    // Handle status filter
    if (status === 'active') {
        assignmentFilters.active_only = true;
    } else if (status === 'closed') {
        assignmentFilters.active_only = false;
    } else {
        assignmentFilters.active_only = null;
    }
    
    loadAssignments();
}

function clearAssignmentFilters() {
    document.getElementById('filterDeviceSelect').value = '';
    document.getElementById('filterLocationSelect').value = '';
    document.getElementById('filterStatusSelect').value = 'active';
    if (document.getElementById('filterDeviceTypeSelect')) document.getElementById('filterDeviceTypeSelect').value = '';
    if (document.getElementById('filterFacilitySelect')) document.getElementById('filterFacilitySelect').value = '';
    
    assignmentFilters.device_id = null;
    assignmentFilters.location_id = null;
    assignmentFilters.device_type = null;
    assignmentFilters.facility_id = null;
    assignmentFilters.active_only = true;
    
    loadAssignments();
}

document.getElementById('assignmentForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const formData = new FormData(e.target);
    
    // Convert datetime-local to ISO format
    const assignDate = new Date(formData.get('assign_date')).toISOString();
    const endDate = formData.get('end_date') ? new Date(formData.get('end_date')).toISOString() : null;
    
    const data = {
        device_id: parseInt(formData.get('device_id')),
        facility_id: parseInt(formData.get('facility_id')),
        space_id: formData.get('space_id') ? parseInt(formData.get('space_id')) : null,
        assign_date: assignDate,
        end_date: endDate,
        workorder_assign_id: formData.get('workorder_assign_id') ? parseInt(formData.get('workorder_assign_id')) : null,
        notes: formData.get('notes') || null,
        program_id: parseInt(formData.get('program_id'))
    };
    
    try {
        const response = await fetch('/api/v1/admin/device-assignments', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });
        
        if (response.ok) {
            showMessage('Device assignment created successfully!', 'success');
            e.target.reset();
            loadAssignments();
        } else {
            const error = await response.json();
            showMessage(error.detail || 'Failed to create assignment', 'error');
        }
    } catch (error) {
        showMessage('Error creating assignment', 'error');
    }
});

async function loadAssignments() {
    const loading = document.getElementById('assignmentsLoading');
    const table = document.getElementById('assignmentsTable');
    const tbody = document.getElementById('assignmentsTableBody');
    const filterStatus = document.getElementById('assignmentFilterStatus');
    const countDisplay = document.getElementById('assignmentCount');
    
    loading.classList.add('show');
    table.style.display = 'none';
    
    // Build query parameters
    const params = new URLSearchParams({
        limit: '1000'
    });
    
    if (assignmentFilters.device_id) {
        params.append('device_id', assignmentFilters.device_id);
    }
    
    if (assignmentFilters.location_id) {
        params.append('location_id', assignmentFilters.location_id);
    }
    
    if (assignmentFilters.active_only !== null) {
        params.append('active_only', assignmentFilters.active_only);
    }
    
    try {
        const response = await fetch(`/api/v1/admin/device-assignments?${params.toString()}`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (response.ok) {
            let assignments = await response.json();
            // Handle response wrapping (could be array or {data: array})
            if (!Array.isArray(assignments)) {
                assignments = assignments.data || [];
            }
            
            // Load admin metadata (devices, locations, facilities, device types)
            const meta = await loadAdminMetadata();
            const devices = meta.devices || [];
            const locations = meta.locations || [];
            const deviceMap = Object.fromEntries(devices.map(d => [d.device_id, d]));
            const locationMap = Object.fromEntries(locations.map(l => [l.location_id, l]));

            // Populate filter dropdowns if not already done
            await populateAssignmentFilterDropdowns(devices, locations, meta.device_types, meta.facilities);
            
            // Update filter status message
            let filterMessage = '';
            const activeFilters = [];
            
            if (assignmentFilters.device_id) {
                const device = deviceMap[assignmentFilters.device_id];
                activeFilters.push(`Device: ${device?.device_terminal_id}`);
            }
            
            if (assignmentFilters.location_id) {
                const location = locationMap[assignmentFilters.location_id];
                activeFilters.push(`Location: ${location?.facility_name}`);
            }
            
            if (assignmentFilters.active_only === true) {
                activeFilters.push('Status: Active');
            } else if (assignmentFilters.active_only === false) {
                activeFilters.push('Status: Closed');
            }
            
            if (activeFilters.length > 0) {
                filterMessage = `(Filtered: ${activeFilters.join(', ')})`;
            }
            
            // Client-side filtering: fetch assignments (unfiltered) then apply filters locally
            let fetched = assignments;
            // apply assignmentFilters
            const filtered = fetched.filter(a => {
                // device_id filter
                if (assignmentFilters.device_id && a.device_id !== assignmentFilters.device_id) return false;
                // location_id filter
                if (assignmentFilters.location_id && a.location_id !== assignmentFilters.location_id) return false;
                // device_type filter
                if (assignmentFilters.device_type) {
                    const dv = deviceMap[a.device_id];
                    if (!dv || dv.device_type !== assignmentFilters.device_type) return false;
                }
                // facility filter
                if (assignmentFilters.facility_id) {
                    const loc = locationMap[a.location_id];
                    if (!loc || loc.facility_id !== assignmentFilters.facility_id) return false;
                }
                // status filter
                if (assignmentFilters.active_only === true && a.end_date) return false;
                if (assignmentFilters.active_only === false && !a.end_date) return false;
                return true;
            });

            filterStatus.textContent = filterMessage;
            countDisplay.textContent = `Showing ${filtered.length} assignment${filtered.length !== 1 ? 's' : ''}`;

            tbody.innerHTML = filtered.map(assignment => {
                const device = deviceMap[assignment.device_id];
                const location = locationMap[assignment.location_id];

                return `
                    <tr>
                        <td><strong>${device ? device.device_terminal_id : assignment.device_id}</strong></td>
                        <td>${device ? device.device_type : 'N/A'}</td>
                        <td>${location ? `${location.facility_name}${location.space_number ? ` - Space ${location.space_number}` : ''}` : assignment.location_id}</td>
                        <td>${new Date(assignment.assign_date).toLocaleDateString()}</td>
                        <td>${assignment.end_date ? new Date(assignment.end_date).toLocaleDateString() : 'Active'}</td>
                        <td>
                            ${assignment.end_date ? '<span class="badge badge-warning">Closed</span>' : '<span class="badge badge-success">Active</span>'}
                        </td>
                        <td>
                            <button class="btn-secondary action-btn" onclick="editAssignment(${assignment.assignment_id})">Edit</button>
                            ${!assignment.end_date ? `<button class="btn-danger action-btn" onclick="closeAssignment(${assignment.assignment_id})">Close</button>` : ''}
                        </td>
                    </tr>
                `;
            }).join('');

            table.style.display = 'table';
        }
    } catch (error) {
        showMessage('Error loading assignments', 'error');
    } finally {
        loading.classList.remove('show');
    }
}

async function populateAssignmentFilterDropdowns(devices, locations, deviceTypes = [], facilities = []) {
    // Devices dropdown
    const deviceSelect = document.getElementById('filterDeviceSelect');
    if (deviceSelect && deviceSelect.options.length <= 1) {
        const deviceOptions = devices
            .map(d => `<option value="${d.device_id}">${d.device_terminal_id} (${d.device_type})</option>`)
            .join('');
        deviceSelect.innerHTML = '<option value="">All Devices</option>' + deviceOptions;
    }

    // Device type dropdown
    const deviceTypeSelect = document.getElementById('filterDeviceTypeSelect');
    if (deviceTypeSelect && deviceTypeSelect.options.length <= 1) {
        const typeOptions = deviceTypes.map(t => `<option value="${t}">${t}</option>`).join('');
        deviceTypeSelect.innerHTML = '<option value="">All Types</option>' + typeOptions;
    }

    // Facility dropdown
    const facilitySelect = document.getElementById('filterFacilitySelect');
    if (facilitySelect && facilitySelect.options.length <= 1) {
        const facOptions = facilities.map(f => `<option value="${f.facility_id}">${f.facility_name}</option>`).join('');
        facilitySelect.innerHTML = '<option value="">All Facilities</option>' + facOptions;
    }

    // Locations dropdown
    const locationSelect = document.getElementById('filterLocationSelect');
    if (locationSelect && locationSelect.options.length <= 1) {
        const locationOptions = locations
            .map(l => `<option value="${l.location_id}">${l.facility_name}${l.space_number ? ` - Space ${l.space_number}` : ''}</option>`)
            .join('');
        locationSelect.innerHTML = '<option value="">All Locations</option>' + locationOptions;
    }
}

async function loadDevicesForDropdown() {
    try {
        const meta = await loadAdminMetadata();
        const devices = meta.devices || [];
        const select = document.getElementById('assignDeviceSelect');
        select.innerHTML = '<option value="">Select device...</option>' +
            devices.map(d => `<option value="${d.device_id}">${d.device_terminal_id} (${d.device_type})</option>`).join('');
    } catch (error) {
        console.error('Error loading devices:', error);
    }
}

async function loadFacilitiesForDropdown() {
    try {
        const meta = await loadAdminMetadata();
        const facilities = meta.facilities || [];
        const select = document.getElementById('assignFacilitySelect');
        select.innerHTML = '<option value="">Select facility...</option>' +
            facilities.map(f => `<option value="${f.facility_id}">${f.facility_name}</option>`).join('');
    } catch (error) {
        console.error('Error loading facilities:', error);
    }
}

async function loadSpacesForFacility(facilityId) {
    const select = document.getElementById('assignSpaceSelect');
    select.innerHTML = '<option value="">Loading...</option>';
    
    if (!facilityId) {
        select.innerHTML = '<option value="">Select facility first</option>';
        return;
    }
    
    try {
        const response = await fetch(`/api/v1/admin/spaces?facility_id=${facilityId}`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (response.ok) {
            const spaces = await response.json();
            select.innerHTML = '<option value="">No space (facility only)</option>' +
                spaces.map(s => `<option value="${s.space_id}">${s.space_number || s.space_id}</option>`).join('');
        }
    } catch (error) {
        select.innerHTML = '<option value="">Error loading spaces</option>';
    }
}

async function editAssignment(assignmentId) {
    // Load assignment details
    try {
        const response = await fetch('/api/v1/admin/device-assignments', {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (response.ok) {
            const assignments = await response.json();
            const assignment = assignments.find(a => a.assignment_id === assignmentId);
            
            if (assignment) {
                // Load locations for dropdown
                await loadLocationsForEdit();
                
                // Populate form
                const form = document.getElementById('editAssignmentForm');
                form.elements['assignment_id'].value = assignment.assignment_id;
                form.elements['location_id'].value = assignment.location_id;
                
                if (assignment.assign_date) {
                    form.elements['assign_date'].value = new Date(assignment.assign_date).toISOString().slice(0, 16);
                }
                
                if (assignment.end_date) {
                    form.elements['end_date'].value = new Date(assignment.end_date).toISOString().slice(0, 16);
                }
                
                form.elements['workorder_assign_id'].value = assignment.workorder_assign_id || '';
                form.elements['workorder_remove_id'].value = assignment.workorder_remove_id || '';
                form.elements['notes'].value = assignment.notes || '';
                
                // Show modal
                document.getElementById('editAssignmentModal').classList.add('show');
            }
        }
    } catch (error) {
        showMessage('Error loading assignment details', 'error');
    }
}

async function loadLocationsForEdit() {
    try {
        const response = await fetch('/api/v1/admin/locations', {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (response.ok) {
            const locations = await response.json();
            const select = document.getElementById('editLocationSelect');
            
            select.innerHTML = '<option value="">Select location...</option>' +
                locations.map(l => `<option value="${l.location_id}">${l.facility_name}${l.space_number ? ` - Space ${l.space_number}` : ''}</option>`).join('');
        }
    } catch (error) {
        console.error('Error loading locations:', error);
    }
}

document.getElementById('editAssignmentForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const formData = new FormData(e.target);
    const assignmentId = formData.get('assignment_id');
    
    const data = {};
    
    if (formData.get('location_id')) data.location_id = parseInt(formData.get('location_id'));
    if (formData.get('assign_date')) data.assign_date = new Date(formData.get('assign_date')).toISOString();
    if (formData.get('end_date')) data.end_date = new Date(formData.get('end_date')).toISOString();
    if (formData.get('workorder_assign_id')) data.workorder_assign_id = parseInt(formData.get('workorder_assign_id'));
    if (formData.get('workorder_remove_id')) data.workorder_remove_id = parseInt(formData.get('workorder_remove_id'));
    if (formData.get('notes')) data.notes = formData.get('notes');
    
    try {
        const response = await fetch(`/api/v1/admin/device-assignments/${assignmentId}`, {
            method: 'PUT',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });
        
        if (response.ok) {
            showMessage('Assignment updated successfully!', 'success');
            closeEditModal();
            loadAssignments();
        } else {
            const error = await response.json();
            showMessage(error.detail || 'Failed to update assignment', 'error');
        }
    } catch (error) {
        showMessage('Error updating assignment', 'error');
    }
});

function closeEditModal() {
    document.getElementById('editAssignmentModal').classList.remove('show');
}

function closeAssignment(assignmentId) {
    // Set assignment ID and show modal
    document.getElementById('closeAssignmentForm').elements['assignment_id'].value = assignmentId;
    
    // Set default end date to now
    document.getElementById('closeAssignmentForm').elements['end_date'].value = 
        new Date().toISOString().slice(0, 16);
    
    document.getElementById('closeAssignmentModal').classList.add('show');
}

document.getElementById('closeAssignmentForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const formData = new FormData(e.target);
    const assignmentId = formData.get('assignment_id');
    const endDate = new Date(formData.get('end_date')).toISOString();
    const workorderRemoveId = formData.get('workorder_remove_id') ? parseInt(formData.get('workorder_remove_id')) : null;
    const notes = formData.get('notes') || null;
    
    try {
        const response = await fetch(`/api/v1/admin/device-assignments/${assignmentId}/close?end_date=${encodeURIComponent(endDate)}${workorderRemoveId ? `&workorder_remove_id=${workorderRemoveId}` : ''}${notes ? `&notes=${encodeURIComponent(notes)}` : ''}`, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${token}`
            }
        });
        
        if (response.ok) {
            showMessage('Assignment closed successfully!', 'success');
            closeCloseModal();
            loadAssignments();
        } else {
            const error = await response.json();
            showMessage(error.detail || 'Failed to close assignment', 'error');
        }
    } catch (error) {
        showMessage('Error closing assignment', 'error');
    }
});

function closeCloseModal() {
    document.getElementById('closeAssignmentModal').classList.remove('show');
}

// ============= Settlement System Management =============

document.getElementById('settlementForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const formData = new FormData(e.target);
    const data = {
        system_name: formData.get('system_name'),
        system_type: formData.get('system_type') || null
    };
    
    try {
        const response = await fetch('/api/v1/admin/settlement-systems', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });
        
        if (response.ok) {
            showMessage('Settlement system created successfully!', 'success');
            e.target.reset();
            loadSettlementSystems();
        } else {
            const error = await response.json();
            showMessage(error.detail || 'Failed to create settlement system', 'error');
        }
    } catch (error) {
        showMessage('Error creating settlement system', 'error');
    }
});

async function loadSettlementSystems() {
    const loading = document.getElementById('settlementLoading');
    const table = document.getElementById('settlementTable');
    const tbody = document.getElementById('settlementTableBody');
    
    loading.classList.add('show');
    table.style.display = 'none';
    
    try {
        const response = await fetch('/api/v1/admin/settlement-systems', {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (response.ok) {
            const systems = await response.json();
            
            tbody.innerHTML = systems.map(system => `
                <tr>
                    <td>${system.settlement_system_id}</td>
                    <td><strong>${system.system_name}</strong></td>
                    <td>${system.system_type || '-'}</td>
                </tr>
            `).join('');
            
            table.style.display = 'table';
        }
    } catch (error) {
        showMessage('Error loading settlement systems', 'error');
    } finally {
        loading.classList.remove('show');
    }
}

// ============= Payment Method Management =============

document.getElementById('paymentForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const formData = new FormData(e.target);
    const data = {
        payment_method_brand: formData.get('payment_method_brand'),
        payment_method_type: formData.get('payment_method_type'),
        is_cash: formData.get('is_cash') === 'on',
        is_card: formData.get('is_card') === 'on',
        is_mobile: formData.get('is_mobile') === 'on',
        is_check: formData.get('is_check') === 'on'
    };
    
    try {
        const response = await fetch('/api/v1/admin/payment-methods', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });
        
        if (response.ok) {
            showMessage('Payment method created successfully!', 'success');
            e.target.reset();
            loadPaymentMethods();
        } else {
            const error = await response.json();
            showMessage(error.detail || 'Failed to create payment method', 'error');
        }
    } catch (error) {
        showMessage('Error creating payment method', 'error');
    }
});

async function loadPaymentMethods() {
    const loading = document.getElementById('paymentLoading');
    const table = document.getElementById('paymentTable');
    const tbody = document.getElementById('paymentTableBody');
    
    loading.classList.add('show');
    table.style.display = 'none';
    
    try {
        const response = await fetch('/api/v1/admin/payment-methods', {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (response.ok) {
            const methods = await response.json();
            
            tbody.innerHTML = methods.map(method => `
                <tr>
                    <td>${method.payment_method_id}</td>
                    <td><strong>${method.payment_method_brand}</strong></td>
                    <td>${method.payment_method_type}</td>
                    <td>
                        ${method.is_cash ? '<span class="badge badge-success">Cash</span>' : ''}
                        ${method.is_card ? '<span class="badge badge-info">Card</span>' : ''}
                        ${method.is_mobile ? '<span class="badge badge-warning">Mobile</span>' : ''}
                        ${method.is_check ? '<span class="badge badge-info">Check</span>' : ''}
                    </td>
                </tr>
            `).join('');
            
            table.style.display = 'table';
        }
    } catch (error) {
        showMessage('Error loading payment methods', 'error');
    } finally {
        loading.classList.remove('show');
    }
}

// ============= Initialization =============

document.addEventListener('DOMContentLoaded', () => {
    loadUserInfo();
    loadDevices();
});