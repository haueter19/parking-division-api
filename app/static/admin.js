/**
 * Admin Configuration JavaScript - UPDATED VERSION
 * Loads all data once on page load, handles filtering on client-side
 * Includes searchable dropdowns using vanilla JavaScript
 */

const token = localStorage.getItem('access_token');
if (!token) {
    window.location.href = '/';
}

// ============= Global Data Cache =============

let adminData = {
    devices: [],
    locations: [],
    facilities: [],
    spaces: [],
    device_types: [],
    settlement_systems: [],
    payment_methods: [],
    device_assignments: [],
};

let currentEditAssignment = null;
let dataLoaded = false;

function populateSpaceFacilityDropdown() {
    const select = document.getElementById('spaceFacilitySelect');
    if (select && adminData && adminData.facilities) {
        select.innerHTML = '<option value="">Select facility...</option>' +
            adminData.facilities.map(f => 
                `<option value="${f.facility_id}">${f.facility_name}</option>`
            ).join('');
    }
}
// ============= Initialization =============

document.addEventListener('DOMContentLoaded', async () => {
    await loadUserInfo();
    await loadAllData();
    showTab('devices');
});

async function loadAllData() {
    try {
        const response = await fetch('/api/v1/admin/metadata', {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (response.ok) {
            adminData = await response.json();
            dataLoaded = true;
            console.log('Admin data loaded:', {
                devices: adminData.devices.length,
                spaces: adminData.spaces.length,
                assignments: adminData.device_assignments.length
            });
        } else {
            showMessage('Failed to load admin data', 'error');
        }
    } catch (error) {
        console.error('Error loading admin data:', error);
        showMessage('Error loading admin data', 'error');
    }
}

// ============= Searchable Dropdown Component =============

function createSearchableDropdown(selectElement, items, valueKey, displayFunction) {
    const wrapper = document.createElement('div');
    wrapper.className = 'searchable-dropdown';
    wrapper.style.position = 'relative';
    
    const input = document.createElement('input');
    input.type = 'text';
    input.placeholder = 'Type to search...';
    input.className = 'searchable-input';
    
    const dropdown = document.createElement('div');
    dropdown.className = 'searchable-options';
    dropdown.style.display = 'none';
    dropdown.style.position = 'absolute';
    dropdown.style.top = '100%';
    dropdown.style.left = '0';
    dropdown.style.right = '0';
    dropdown.style.maxHeight = '200px';
    dropdown.style.overflowY = 'auto';
    dropdown.style.background = 'white';
    dropdown.style.border = '1px solid #d1d5db';
    dropdown.style.borderRadius = '4px';
    dropdown.style.zIndex = '1000';
    dropdown.style.marginTop = '2px';
    
    let selectedValue = '';
    
    function renderOptions(filter = '') {
        dropdown.innerHTML = '';
        const filtered = items.filter(item => {
            const display = displayFunction(item).toLowerCase();
            return display.includes(filter.toLowerCase());
        });
        
        filtered.forEach(item => {
            const option = document.createElement('div');
            option.className = 'searchable-option';
            option.textContent = displayFunction(item);
            option.style.padding = '8px 12px';
            option.style.cursor = 'pointer';
            option.dataset.value = item[valueKey];
            
            option.addEventListener('mouseenter', () => {
                option.style.background = '#f3f4f6';
            });
            
            option.addEventListener('mouseleave', () => {
                option.style.background = 'white';
            });
            
            option.addEventListener('click', () => {
                selectedValue = item[valueKey];
                input.value = displayFunction(item);
                selectElement.value = selectedValue;
                selectElement.dispatchEvent(new Event('change'));
                dropdown.style.display = 'none';
            });
            
            dropdown.appendChild(option);
        });
        
        if (filtered.length === 0) {
            const noResults = document.createElement('div');
            noResults.textContent = 'No results found';
            noResults.style.padding = '8px 12px';
            noResults.style.color = '#6b7280';
            dropdown.appendChild(noResults);
        }
    }
    
    input.addEventListener('focus', () => {
        renderOptions(input.value);
        dropdown.style.display = 'block';
    });
    
    input.addEventListener('input', (e) => {
        renderOptions(e.target.value);
        dropdown.style.display = 'block';
    });
    
    input.addEventListener('blur', () => {
        // Delay to allow click events on options
        setTimeout(() => {
            dropdown.style.display = 'none';
        }, 200);
    });
    
    wrapper.appendChild(input);
    wrapper.appendChild(dropdown);
    
    // Replace select with wrapper
    selectElement.style.display = 'none';
    selectElement.parentNode.insertBefore(wrapper, selectElement);
    
    return { input, dropdown, wrapper };
}

// ============= Tab Management =============

function showTab(tabName, event) {  // <-- Add event parameter
    if (!dataLoaded) {
        showMessage('Loading data, please wait...', 'info');
        return;
    }
    
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
    
    // Only try to access event.target if event exists
    if (event && event.target) {
        event.target.classList.add('active');
    } else {
        // If no event (called programmatically), find and activate the button
        document.querySelectorAll('.tab').forEach(btn => {
            if (btn.textContent.toLowerCase().includes(tabName.toLowerCase())) {
                btn.classList.add('active');
            }
        });
    }
    
    // Render data for the tab
    switch(tabName) {
        case 'devices':
            renderDevices(adminData.devices);
            break;
        case 'assignments':
            renderAssignments(adminData.device_assignments);
            setupAssignmentFilters();
            break;
        case 'settlement':
            renderSettlementSystems(adminData.settlement_systems);
            break;
        case 'payment':
            renderPaymentMethods(adminData.payment_methods);
            break;
        case 'spaces':
            populateSpaceFacilityDropdown();
            renderSpaces(adminData.spaces);
            setupSpaceFilters();
            break;
    }
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

// ============= Devices Tab =============

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
            await loadAllData();
            renderDevices(adminData.devices);
        } else {
            const error = await response.json();
            showMessage(error.detail || 'Failed to create device', 'error');
        }
    } catch (error) {
        showMessage('Error creating device', 'error');
    }
});

function renderDevices(devices) {
    const tbody = document.getElementById('devicesTableBody');
    const table = document.getElementById('devicesTable');
    const loading = document.getElementById('devicesLoading');
    
    loading.classList.remove('show');
    
    if (!devices || devices.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;">No devices found</td></tr>';
        table.style.display = 'table';
        return;
    }
    
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
            <td>${device.Brand || '-'} ${device.Model || ''}</td>
            <td>
                ${device.facility_name ? `<span class="badge badge-info">Assigned: ${device.facility_name}${device.space_number ? ' - ' + device.space_number : ''}</span>` : '<span class="badge">Unassigned</span>'}
            </td>
        </tr>
    `).join('');
    
    table.style.display = 'table';
}

// ============= Assignments Tab =============

let assignmentFilters = {
    device: '',
    deviceType: '',
    facility: '',
    location: '',
    status: 'active'
};

function setupAssignmentFilters() {
    // Device filter
    document.getElementById('filterDeviceInput').addEventListener('input', (e) => {
        assignmentFilters.device = e.target.value;
        applyAssignmentFilters();
    });
    
    // Device type filter
    document.getElementById('filterDeviceTypeSelect').addEventListener('change', (e) => {
        assignmentFilters.deviceType = e.target.value;
        applyAssignmentFilters();
    });
    
    // Facility filter
    document.getElementById('filterFacilityInput').addEventListener('input', (e) => {
        assignmentFilters.facility = e.target.value;
        applyAssignmentFilters();
    });
    
    // Status filter
    document.getElementById('filterStatusSelect').addEventListener('change', (e) => {
        assignmentFilters.status = e.target.value;
        applyAssignmentFilters();
    });
    
    // Populate device type dropdown
    const deviceTypeSelect = document.getElementById('filterDeviceTypeSelect');
    deviceTypeSelect.innerHTML = '<option value="">All Types</option>' +
        adminData.device_types.map(t => `<option value="${t}">${t}</option>`).join('');
}

function applyAssignmentFilters() {
    let filtered = [...adminData.device_assignments];
    
    // Filter by device
    if (assignmentFilters.device) {
        filtered = filtered.filter(a => 
            a.device_terminal_id.toLowerCase().includes(assignmentFilters.device.toLowerCase())
        );
    }
    
    // Filter by device type
    if (assignmentFilters.deviceType) {
        filtered = filtered.filter(a => a.device_type === assignmentFilters.deviceType);
    }
    
    // Filter by facility
    if (assignmentFilters.facility) {
        filtered = filtered.filter(a => 
            a.facility_name.toLowerCase().includes(assignmentFilters.facility.toLowerCase())
        );
    }
    
    // Filter by status
    if (assignmentFilters.status === 'active') {
        filtered = filtered.filter(a => !a.end_date);
    } else if (assignmentFilters.status === 'closed') {
        filtered = filtered.filter(a => a.end_date);
    }
    
    renderAssignments(filtered);
    
    // Update filter status
    const activeFilters = [];
    if (assignmentFilters.device) activeFilters.push(`Device: ${assignmentFilters.device}`);
    if (assignmentFilters.deviceType) activeFilters.push(`Type: ${assignmentFilters.deviceType}`);
    if (assignmentFilters.facility) activeFilters.push(`Facility: ${assignmentFilters.facility}`);
    if (assignmentFilters.status !== 'all') activeFilters.push(`Status: ${assignmentFilters.status}`);
    
    document.getElementById('assignmentFilterStatus').textContent = 
        activeFilters.length > 0 ? `(Filtered: ${activeFilters.join(', ')})` : '';
    
    document.getElementById('assignmentCount').textContent = 
        `Showing ${filtered.length} of ${adminData.device_assignments.length} assignments`;
}

function clearAssignmentFilters() {
    assignmentFilters = {
        device: '',
        deviceType: '',
        facility: '',
        location: '',
        status: 'active'
    };
    
    document.getElementById('filterDeviceInput').value = '';
    document.getElementById('filterDeviceTypeSelect').value = '';
    document.getElementById('filterFacilityInput').value = '';
    document.getElementById('filterStatusSelect').value = 'active';
    
    applyAssignmentFilters();
}

function renderAssignments(assignments) {
    const tbody = document.getElementById('assignmentsTableBody');
    const table = document.getElementById('assignmentsTable');
    const loading = document.getElementById('assignmentsLoading');
    
    loading.classList.remove('show');
    
    if (!assignments || assignments.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;">No assignments found</td></tr>';
        table.style.display = 'table';
        return;
    }
    
    tbody.innerHTML = assignments.map(assignment => `
        <tr>
            <td><strong>${assignment.device_terminal_id}</strong></td>
            <td>${assignment.device_type}</td>
            <td>${assignment.facility_name}${assignment.space_number ? ' - Space ' + assignment.space_number : ''}</td>
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
    `).join('');
    
    table.style.display = 'table';
}

// ============= Spaces Tab (NEW) =============

let spaceFilters = {
    facility: '',
    spaceNumber: '',
    spaceType: '',
    status: 'active'
};

function setupSpaceFilters() {
    // Facility filter
    document.getElementById('filterSpaceFacilityInput').addEventListener('input', (e) => {
        spaceFilters.facility = e.target.value;
        applySpaceFilters();
    });
    
    // Space number filter
    document.getElementById('filterSpaceNumberInput').addEventListener('input', (e) => {
        spaceFilters.spaceNumber = e.target.value;
        applySpaceFilters();
    });
    
    // Space type filter
    document.getElementById('filterSpaceTypeInput').addEventListener('input', (e) => {
        spaceFilters.spaceType = e.target.value;
        applySpaceFilters();
    });
    
    // Status filter
    document.getElementById('filterSpaceStatusSelect').addEventListener('change', (e) => {
        spaceFilters.status = e.target.value;
        applySpaceFilters();
    });
}

function applySpaceFilters() {
    let filtered = [...adminData.spaces];
    
    // Filter by facility
    if (spaceFilters.facility) {
        filtered = filtered.filter(s => 
            s.facility_name.toLowerCase().includes(spaceFilters.facility.toLowerCase())
        );
    }
    
    // Filter by space number
    if (spaceFilters.spaceNumber) {
        filtered = filtered.filter(s => 
            s.space_number && s.space_number.toLowerCase().includes(spaceFilters.spaceNumber.toLowerCase())
        );
    }
    
    // Filter by space type
    if (spaceFilters.spaceType) {
        filtered = filtered.filter(s => 
            s.space_type && s.space_type.toLowerCase().includes(spaceFilters.spaceType.toLowerCase())
        );
    }
    
    // Filter by status
    if (spaceFilters.status === 'active') {
        filtered = filtered.filter(s => !s.end_date);
    } else if (spaceFilters.status === 'closed') {
        filtered = filtered.filter(s => s.end_date);
    }
    
    renderSpaces(filtered);
    
    // Update filter status
    const activeFilters = [];
    if (spaceFilters.facility) activeFilters.push(`Facility: ${spaceFilters.facility}`);
    if (spaceFilters.spaceNumber) activeFilters.push(`Space: ${spaceFilters.spaceNumber}`);
    if (spaceFilters.spaceType) activeFilters.push(`Type: ${spaceFilters.spaceType}`);
    if (spaceFilters.status !== 'all') activeFilters.push(`Status: ${spaceFilters.status}`);
    
    document.getElementById('spaceFilterStatus').textContent = 
        activeFilters.length > 0 ? `(Filtered: ${activeFilters.join(', ')})` : '';
    
    document.getElementById('spaceCount').textContent = 
        `Showing ${filtered.length} of ${adminData.spaces.length} spaces`;
}

function clearSpaceFilters() {
    spaceFilters = {
        facility: '',
        spaceNumber: '',
        spaceType: '',
        status: 'active'
    };
    
    document.getElementById('filterSpaceFacilityInput').value = '';
    document.getElementById('filterSpaceNumberInput').value = '';
    document.getElementById('filterSpaceTypeInput').value = '';
    document.getElementById('filterSpaceStatusSelect').value = 'active';
    
    applySpaceFilters();
}

function renderSpaces(spaces) {
    const tbody = document.getElementById('spacesTableBody');
    const table = document.getElementById('spacesTable');
    const loading = document.getElementById('spacesLoading');
    
    loading.classList.remove('show');
    
    if (!spaces || spaces.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;">No spaces found</td></tr>';
        table.style.display = 'table';
        return;
    }
    
    tbody.innerHTML = spaces.map(space => `
        <tr>
            <td>${space.space_id}</td>
            <td><strong>${space.space_number}</strong></td>
            <td>${space.space_type || '-'}</td>
            <td>${space.facility_name}</td>
            <td>${new Date(space.start_date).toLocaleDateString()}</td>
            <td>${space.end_date ? new Date(space.end_date).toLocaleDateString() : 'Active'}</td>
            <td>
                ${space.end_date ? '<span class="badge badge-warning">Closed</span>' : '<span class="badge badge-success">Active</span>'}
            </td>
        </tr>
    `).join('');
    
    table.style.display = 'table';
}

document.getElementById('spaceForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const formData = new FormData(e.target);
    const data = {
        space_number: formData.get('space_number'),
        space_type: formData.get('space_type'),
        facility_id: parseInt(formData.get('facility_id')),
        cwAssetID: formData.get('cwAssetID') || null,
        start_date: formData.get('start_date'),
        space_status: formData.get('space_status') || 'Active'
    };
    
    try {
        const response = await fetch('/api/v1/admin/spaces', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });
        
        if (response.ok) {
            showMessage('Space created successfully!', 'success');
            e.target.reset();
            await loadAllData();
            renderSpaces(adminData.spaces);
        } else {
            const error = await response.json();
            showMessage(error.detail || 'Failed to create space', 'error');
        }
    } catch (error) {
        showMessage('Error creating space', 'error');
    }
});

// ============= Settlement Systems Tab =============

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
            await loadAllData();
            renderSettlementSystems(adminData.settlement_systems);
        } else {
            const error = await response.json();
            showMessage(error.detail || 'Failed to create settlement system', 'error');
        }
    } catch (error) {
        showMessage('Error creating settlement system', 'error');
    }
});

function renderSettlementSystems(systems) {
    const tbody = document.getElementById('settlementTableBody');
    const table = document.getElementById('settlementTable');
    const loading = document.getElementById('settlementLoading');
    
    loading.classList.remove('show');
    
    if (!systems || systems.length === 0) {
        tbody.innerHTML = '<tr><td colspan="3" style="text-align:center;">No settlement systems found</td></tr>';
        table.style.display = 'table';
        return;
    }
    
    tbody.innerHTML = systems.map(system => `
        <tr>
            <td>${system.settlement_system_id}</td>
            <td><strong>${system.system_name}</strong></td>
            <td>${system.system_type || '-'}</td>
        </tr>
    `).join('');
    
    table.style.display = 'table';
}

// ============= Payment Methods Tab =============

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
            await loadAllData();
            renderPaymentMethods(adminData.payment_methods);
        } else {
            const error = await response.json();
            showMessage(error.detail || 'Failed to create payment method', 'error');
        }
    } catch (error) {
        showMessage('Error creating payment method', 'error');
    }
});

function renderPaymentMethods(methods) {
    const tbody = document.getElementById('paymentTableBody');
    const table = document.getElementById('paymentTable');
    const loading = document.getElementById('paymentLoading');
    
    loading.classList.remove('show');
    
    if (!methods || methods.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;">No payment methods found</td></tr>';
        table.style.display = 'table';
        return;
    }
    
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


function editAssignment(assignmentId) {
    // Find the assignment in our cached data
    const assignment = adminData.device_assignments.find(a => a.assignment_id === assignmentId);
    
    if (!assignment) {
        showMessage('Assignment not found', 'error');
        return;
    }
    
    currentEditAssignment = assignment;
    
    // Populate the edit modal
    const form = document.getElementById('editAssignmentForm');
    form.elements['assignment_id'].value = assignment.assignment_id;
    
    // Populate location dropdown
    populateEditLocationDropdown();
    
    // Set current location
    setTimeout(() => {
        form.elements['location_id'].value = assignment.location_id;
    }, 100);
    
    // Set dates
    if (assignment.assign_date) {
        const assignDate = new Date(assignment.assign_date);
        form.elements['assign_date'].value = formatDateTimeLocal(assignDate);
    }
    
    if (assignment.end_date) {
        const endDate = new Date(assignment.end_date);
        form.elements['end_date'].value = formatDateTimeLocal(endDate);
    } else {
        form.elements['end_date'].value = '';
    }
    
    // Set workorder IDs
    form.elements['workorder_assign_id'].value = assignment.workorder_assign_id || '';
    form.elements['workorder_remove_id'].value = assignment.workorder_remove_id || '';
    
    // Set notes
    form.elements['notes'].value = assignment.notes || '';
    
    // Show modal
    document.getElementById('editAssignmentModal').classList.add('show');
}

function populateEditLocationDropdown() {
    const select = document.getElementById('editLocationSelect');
    
    if (!adminData || !adminData.locations) {
        select.innerHTML = '<option value="">No locations available</option>';
        return;
    }
    
    // Group locations by facility for better UX
    const locationsByFacility = {};
    adminData.locations.forEach(loc => {
        if (!locationsByFacility[loc.facility_name]) {
            locationsByFacility[loc.facility_name] = [];
        }
        locationsByFacility[loc.facility_name].push(loc);
    });
    
    let optionsHtml = '<option value="">Select location...</option>';
    
    Object.keys(locationsByFacility).sort().forEach(facilityName => {
        const locations = locationsByFacility[facilityName];
        
        locations.forEach(loc => {
            const label = loc.space_number 
                ? `${loc.location_id} - ${facilityName} - Space ${loc.space_number}`
                : facilityName;
            optionsHtml += `<option value="${loc.location_id}">${label}</option>`;
        });
    });
    
    select.innerHTML = optionsHtml;
}

function closeEditModal() {
    document.getElementById('editAssignmentModal').classList.remove('show');
    currentEditAssignment = null;
}

// Helper function to format date for datetime-local input
function formatDateTimeLocal(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    return `${year}-${month}-${day}T${hours}:${minutes}`;
}

// Handle edit form submission
document.getElementById('editAssignmentForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const formData = new FormData(e.target);
    const assignmentId = parseInt(formData.get('assignment_id'));
    
    // Build update payload - only include changed fields
    const data = {};
    
    const locationId = formData.get('location_id');
    if (locationId && parseInt(locationId) !== currentEditAssignment.location_id) {
        data.location_id = parseInt(locationId);
    }
    
    const assignDate = formData.get('assign_date');
    if (assignDate) {
        const newAssignDate = new Date(assignDate).toISOString();
        if (newAssignDate !== currentEditAssignment.assign_date) {
            data.assign_date = assignDate;
        }
    }
    
    const endDate = formData.get('end_date');
    if (endDate) {
        data.end_date = endDate;
    }
    
    const workorderAssign = formData.get('workorder_assign_id');
    if (workorderAssign && workorderAssign !== '') {
        data.workorder_assign_id = parseInt(workorderAssign);
    }
    
    const workorderRemove = formData.get('workorder_remove_id');
    if (workorderRemove && workorderRemove !== '') {
        data.workorder_remove_id = parseInt(workorderRemove);
    }
    
    const notes = formData.get('notes');
    if (notes !== (currentEditAssignment.notes || '')) {
        data.notes = notes;
    }
    
    // Check if there are any changes
    if (Object.keys(data).length === 0) {
        showMessage('No changes detected', 'info');
        closeEditModal();
        return;
    }
    
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
            
            // Reload data and re-render
            await loadAllData();
            applyAssignmentFilters();
        } else {
            const error = await response.json();
            showMessage(error.detail || 'Failed to update assignment', 'error');
        }
    } catch (error) {
        console.error('Error updating assignment:', error);
        showMessage('Error updating assignment', 'error');
    }
});

// Close modal when clicking close button
document.querySelector('#editAssignmentModal .close-modal')?.addEventListener('click', closeEditModal);

// Close modal when clicking outside
document.getElementById('editAssignmentModal')?.addEventListener('click', (e) => {
    if (e.target.id === 'editAssignmentModal') {
        closeEditModal();
    }
});