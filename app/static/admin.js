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

let deviceFilters = {
    terminalId: '',
    deviceType: '',
    brand: '',
    assignmentStatus: 'all'  // 'all', 'assigned', 'unassigned'
};

let currentEditAssignment = null;
let dataLoaded = false;
let allUsers = [];
let filteredUsers = [];

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
            setupDeviceFilters();
            break;
        case 'assignments':
            renderAssignments(adminData.device_assignments);
            setupAssignmentFilters();
            populateAssignmentDropdowns();
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
        case 'users':
            loadUsersData();
            setupUserFilters();
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
                ${device.facility_name ? 
                    `<span class="badge badge-info">Assigned: ${device.facility_name}${device.space_number ? ' - ' + device.space_number : ''}</span>` : 
                    '<span class="badge">Unassigned</span>'
                }
                ${!device.assignment_id ? 
                    `<button class="btn-primary action-btn" onclick="switchToAssignmentsTab(${device.device_id})" style="margin-left: 8px;">Assign</button>` :
                    `<button class="btn-secondary action-btn" onclick="switchToAssignmentsTab(${device.device_id})" style="margin-left: 8px;">Reassign</button>`
                }
            </td>
        </tr>
    `).join('');
    
    table.style.display = 'table';
}

/**
 * Setup device filter event listeners
 * Called when the devices tab is shown
 */
function setupDeviceFilters() {
    // Terminal ID filter
    const terminalIdInput = document.getElementById('filterDeviceTerminalIdInput');
    if (terminalIdInput) {
        terminalIdInput.addEventListener('input', (e) => {
            deviceFilters.terminalId = e.target.value;
            applyDeviceFilters();
        });
    }
    
    // Device type filter
    const deviceTypeSelect = document.getElementById('filterDeviceTypeSelectDevices');
    if (deviceTypeSelect) {
        deviceTypeSelect.addEventListener('change', (e) => {
            deviceFilters.deviceType = e.target.value;
            applyDeviceFilters();
        });
        
        // Populate device type dropdown
        deviceTypeSelect.innerHTML = '<option value="">All Types</option>' +
            adminData.device_types.map(t => `<option value="${t}">${t}</option>`).join('');
    }
    
    // Brand filter
    const brandInput = document.getElementById('filterDeviceBrandInput');
    if (brandInput) {
        brandInput.addEventListener('input', (e) => {
            deviceFilters.brand = e.target.value;
            applyDeviceFilters();
        });
    }
    
    // Assignment status filter
    const assignmentStatusSelect = document.getElementById('filterDeviceAssignmentStatusSelect');
    if (assignmentStatusSelect) {
        assignmentStatusSelect.addEventListener('change', (e) => {
            deviceFilters.assignmentStatus = e.target.value;
            applyDeviceFilters();
        });
    }
}

/**
 * Apply current filter settings to devices list
 */
function applyDeviceFilters() {
    let filtered = [...adminData.devices];
    
    // Filter by terminal ID
    if (deviceFilters.terminalId) {
        filtered = filtered.filter(d => 
            d.device_terminal_id.toLowerCase().includes(deviceFilters.terminalId.toLowerCase())
        );
    }
    
    // Filter by device type
    if (deviceFilters.deviceType) {
        filtered = filtered.filter(d => d.device_type === deviceFilters.deviceType);
    }
    
    // Filter by brand
    if (deviceFilters.brand) {
        filtered = filtered.filter(d => 
            d.Brand && d.Brand.toLowerCase().includes(deviceFilters.brand.toLowerCase())
        );
    }
    
    // Filter by assignment status
    if (deviceFilters.assignmentStatus === 'assigned') {
        filtered = filtered.filter(d => d.assignment_id);
    } else if (deviceFilters.assignmentStatus === 'unassigned') {
        filtered = filtered.filter(d => !d.assignment_id);
    }
    
    renderDevices(filtered);
    
    // Update filter status display
    const activeFilters = [];
    if (deviceFilters.terminalId) activeFilters.push(`Terminal: ${deviceFilters.terminalId}`);
    if (deviceFilters.deviceType) activeFilters.push(`Type: ${deviceFilters.deviceType}`);
    if (deviceFilters.brand) activeFilters.push(`Brand: ${deviceFilters.brand}`);
    if (deviceFilters.assignmentStatus !== 'all') activeFilters.push(`Status: ${deviceFilters.assignmentStatus}`);
    
    const filterStatusEl = document.getElementById('deviceFilterStatus');
    if (filterStatusEl) {
        filterStatusEl.textContent = 
            activeFilters.length > 0 ? `(Filtered: ${activeFilters.join(', ')})` : '';
    }
    
    const deviceCountEl = document.getElementById('deviceCount');
    if (deviceCountEl) {
        deviceCountEl.textContent = 
            `Showing ${filtered.length} of ${adminData.devices.length} devices`;
    }
}

/**
 * Clear all device filters
 */
function clearDeviceFilters() {
    deviceFilters = {
        terminalId: '',
        deviceType: '',
        brand: '',
        assignmentStatus: 'all'
    };
    
    // Reset filter inputs
    const terminalIdInput = document.getElementById('filterDeviceTerminalIdInput');
    if (terminalIdInput) terminalIdInput.value = '';
    
    const deviceTypeSelect = document.getElementById('filterDeviceTypeSelectDevices');
    if (deviceTypeSelect) deviceTypeSelect.value = '';
    
    const brandInput = document.getElementById('filterDeviceBrandInput');
    if (brandInput) brandInput.value = '';
    
    const assignmentStatusSelect = document.getElementById('filterDeviceAssignmentStatusSelect');
    if (assignmentStatusSelect) assignmentStatusSelect.value = 'all';
    
    applyDeviceFilters();
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

document.getElementById('assignmentForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const formData = new FormData(e.target);
    
    // Get device_id from either select or datalist input
    let deviceId;
    const deviceInput = document.getElementById('assignDeviceInput');
    if (deviceInput) {
        // Using datalist - need to find device_id from terminal_id
        const terminalId = deviceInput.value;
        const device = adminData.devices.find(d => d.device_terminal_id === terminalId);
        if (!device) {
            showMessage('Please select a valid device from the list', 'error');
            return;
        }
        deviceId = device.device_id;
    } else {
        // Using select
        deviceId = parseInt(formData.get('device_id'));
    }
    
    const data = {
        device_id: deviceId,
        facility_id: parseInt(formData.get('facility_id')),
        space_id: formData.get('space_id') ? parseInt(formData.get('space_id')) : null,
        program_id: formData.get('program_type') ? parseInt(formData.get('program_type')) : 1,
        assign_date: formData.get('assign_date'),
        workorder_assign_id: formData.get('workorder_assign_id') ? parseInt(formData.get('workorder_assign_id')) : null,
        notes: formData.get('notes') || null
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
            await loadAllData();
            applyAssignmentFilters();
        } else {
            const error = await response.json();
            showMessage(error.detail || 'Failed to create assignment', 'error');
        }
    } catch (error) {
        showMessage('Error creating assignment', 'error');
    }
});

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

/**
 * Populate the device and facility dropdowns for device assignment form
 * Called when the assignments tab is shown
 */
function populateAssignmentDropdowns() {
    // Populate facility dropdown
    const facilitySelect = document.getElementById('assignFacilitySelect');
    if (facilitySelect && adminData && adminData.facilities) {
        facilitySelect.innerHTML = '<option value="">Select facility...</option>' +
            adminData.facilities.map(f => 
                `<option value="${f.facility_id}">${f.facility_name}</option>`
            ).join('');
    }
    
    // Populate device datalist - for 1000+ devices
    const deviceDatalist = document.getElementById('assignDeviceDatalist');
    if (deviceDatalist && adminData && adminData.devices) {
        deviceDatalist.innerHTML = adminData.devices.map(d => 
            `<option value="${d.device_terminal_id}">${d.device_terminal_id} (${d.device_type})</option>`
        ).join('');
    }
}

/**
 * Load spaces for a selected facility
 * Called when facility dropdown changes
 */
function loadSpacesForFacility(facilityId) {
    const spaceSelect = document.getElementById('assignSpaceSelect');
    
    if (!facilityId || !adminData || !adminData.spaces) {
        spaceSelect.innerHTML = '<option value="">No space (facility only)</option>';
        return;
    }
    
    // Filter spaces by facility and only show active spaces
    const filteredSpaces = adminData.spaces.filter(s => 
        s.facility_id === parseInt(facilityId) && !s.end_date
    );
    
    spaceSelect.innerHTML = '<option value="">No space (facility only)</option>' +
        filteredSpaces.map(s => 
            `<option value="${s.space_id}">${s.space_number}${s.space_type ? ' (' + s.space_type + ')' : ''}</option>`
        ).join('');
}


/**
 * Switch to assignments tab and start device assignment
 * Called from "Assign" button on Devices tab
 */
function switchToAssignmentsTab(deviceId) {
    // Store the device ID temporarily
    sessionStorage.setItem('pendingAssignmentDeviceId', deviceId);
    
    // Switch to assignments tab
    showTab('assignments');
    
    // After a brief delay to ensure tab is loaded, open the assignment modal
    setTimeout(() => {
        const storedDeviceId = sessionStorage.getItem('pendingAssignmentDeviceId');
        if (storedDeviceId) {
            openAssignmentModal(parseInt(storedDeviceId));
            sessionStorage.removeItem('pendingAssignmentDeviceId');
        }
    }, 100);
}


/**
 * Pre-populate assignment form for a specific device
 * Called when user clicks "Assign" from Devices tab
 */
function openAssignmentModal(deviceId) {
    const device = adminData.devices.find(d => d.device_id === deviceId);
    
    if (!device) {
        showMessage('Device not found', 'error');
        return;
    }
    
    // If device is already assigned, show warning
    if (device.assignment_id) {
        if (!confirm(`This device is currently assigned to ${device.facility_name}${device.space_number ? ' - Space ' + device.space_number : ''}. Do you want to create a new assignment anyway?`)) {
            return;
        }
    }

    // Expand the Create Device Assignment form section if collapsed
    const deviceAssignmentFormSection = document.getElementById('assignmentForm').closest('.form-section');
    const header = deviceAssignmentFormSection.querySelector('.collapsible-header');
    const content = deviceAssignmentFormSection.querySelector('.collapsible-content');
    const icon = header.querySelector('.toggle-icon');
    
    if (!content.classList.contains('expanded')) {
        content.classList.add('expanded');
        icon.textContent = '−';
    }
    
    // Pre-populate the form with device info
    const deviceInput = document.getElementById('assignDeviceInput');
    if (deviceInput) {
        deviceInput.value = device.device_terminal_id;
        deviceInput.dataset.deviceId = device.device_id;
    }
    
    // Clear other fields
    const form = document.getElementById('assignmentForm');
    form.elements['facility_id'].value = '';
    form.elements['space_id'].value = '';
    if (form.elements['program_type']) form.elements['program_type'].value = '1';
    if (form.elements['notes']) form.elements['notes'].value = '';
    
    // Scroll to the assignment form
    form.scrollIntoView({ behavior: 'smooth', block: 'center' });
    
    // Highlight the form briefly
    const formSection = form.closest('.form-section');
    formSection.style.backgroundColor = '#fef3c7';
    setTimeout(() => {
        formSection.style.backgroundColor = '';
    }, 2000);
    
    showMessage(`Ready to assign device ${device.device_terminal_id}`, 'info');
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


// ============= Load Users Data =============
async function loadUsersData() {
    const loading = document.getElementById('usersLoading');
    const table = document.getElementById('usersTable');
    
    if (loading) loading.classList.add('show');
    if (table) table.style.display = 'none';
    
    try {
        const response = await fetch('/api/v1/users', {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (!response.ok) {
            throw new Error('Failed to load users');
        }
        
        allUsers = await response.json();
        filteredUsers = [...allUsers];
        renderUsers(filteredUsers);
        
        if (loading) loading.classList.remove('show');
        if (table) table.style.display = 'table';
        
    } catch (error) {
        console.error('Error loading users:', error);
        showMessage('Failed to load users: ' + error.message, 'error');
        if (loading) loading.classList.remove('show');
    }
}

// ============= Render Users Table =============
function renderUsers(users) {
    const tbody = document.getElementById('usersTableBody');
    
    if (!tbody) {
        console.error('usersTableBody not found');
        return;
    }
    
    if (!users || users.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" style="text-align: center; padding: 20px;">No users found</td></tr>';
        return;
    }
    
    tbody.innerHTML = users.map(user => {
        const statusClass = user.is_active ? 'status-active' : 'status-inactive';
        const statusText = user.is_active ? 'Active' : 'Inactive';
        const roleClass = `role-${user.role.toLowerCase()}`;
        const createdDate = new Date(user.created_at).toLocaleDateString();
        
        const toggleAction = user.is_active 
            ? `<button class="action-button action-deactivate" onclick="toggleUserStatus(${user.id}, false)">Deactivate</button>`
            : `<button class="action-button action-activate" onclick="toggleUserStatus(${user.id}, true)">Activate</button>`;
        
        return `
            <tr>
                <td><strong>${user.username}</strong></td>
                <td>${user.full_name || '<em style="color: #999;">Not set</em>'}</td>
                <td>${user.email}</td>
                <td><span class="role-badge ${roleClass}">${user.role}</span></td>
                <td><span class="status-badge ${statusClass}">${statusText}</span></td>
                <td>${createdDate}</td>
                <td>
                    <button class="action-button action-edit" onclick="openEditUserModal(${user.id})">Edit</button>
                    <button class="action-button action-reset" onclick="openResetPasswordModal(${user.id})">Reset Password</button>
                    ${toggleAction}
                </td>
            </tr>
        `;
    }).join('');
}

// ============= User Filtering =============
function setupUserFilters() {
    const searchInput = document.getElementById('filterUserSearch');
    const roleSelect = document.getElementById('filterUserRole');
    const activeSelect = document.getElementById('filterUserActive');
    
    if (searchInput) {
        searchInput.addEventListener('input', filterUsers);
    }
    if (roleSelect) {
        roleSelect.addEventListener('change', filterUsers);
    }
    if (activeSelect) {
        activeSelect.addEventListener('change', filterUsers);
    }
}

function filterUsers() {
    const search = document.getElementById('filterUserSearch')?.value.toLowerCase() || '';
    const role = document.getElementById('filterUserRole')?.value || '';
    const activeFilter = document.getElementById('filterUserActive')?.value || '';
    
    filteredUsers = allUsers.filter(user => {
        const matchesSearch = !search || 
            user.username.toLowerCase().includes(search) ||
            (user.email && user.email.toLowerCase().includes(search)) ||
            (user.full_name && user.full_name.toLowerCase().includes(search));
        
        const matchesRole = !role || user.role.toLowerCase() === role;
        
        const matchesActive = !activeFilter || 
            (activeFilter === 'true' && user.is_active) ||
            (activeFilter === 'false' && !user.is_active);
        
        return matchesSearch && matchesRole && matchesActive;
    });
    
    renderUsers(filteredUsers);
}

function clearUserFilters() {
    document.getElementById('filterUserSearch').value = '';
    document.getElementById('filterUserRole').value = '';
    document.getElementById('filterUserActive').value = '';
    filterUsers();
}

// ============= Collapsible Section Toggle =============
function toggleSection(sectionId) {
    const content = document.getElementById(`${sectionId}-content`);
    const icon = document.getElementById(`toggle-${sectionId}`);
    
    if (content && icon) {
        if (content.classList.contains('expanded')) {
            content.classList.remove('expanded');
            icon.textContent = '+';
        } else {
            content.classList.add('expanded');
            icon.textContent = '-';
        }
    }
}

// ============= Add New User =============
async function handleAddUserSubmit(e) {
    e.preventDefault();
    
    const username = document.getElementById('newUsername').value.trim();
    const email = document.getElementById('newEmail').value.trim();
    const firstName = document.getElementById('newFirstName').value.trim();
    const lastName = document.getElementById('newLastName').value.trim();
    const fullName = document.getElementById('newFullName').value.trim();
    const role = document.getElementById('newRole').value;
    const password = document.getElementById('newPassword').value;
    const passwordConfirm = document.getElementById('newPasswordConfirm').value;
    
    // Validate passwords match
    if (password !== passwordConfirm) {
        showMessage('Passwords do not match', 'error');
        return;
    }
    
    // Validate password complexity
    if (password.length < 8) {
        showMessage('Password must be at least 8 characters', 'error');
        return;
    }
    
    if (!/[^a-zA-Z0-9]/.test(password)) {
        showMessage('Password must contain at least one non-alphanumeric character', 'error');
        return;
    }
    
    try {
        const response = await fetch('/api/v1/users', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                username,
                email,
                full_name: fullName || null,
                password,
                role
            })
        });
        
        const data = await response.json();
        
        if (response.ok) {
            showMessage(`User '${username}' created successfully!`, 'success');
            document.getElementById('addUserForm').reset();
            
            // Collapse the form
            toggleSection('add-user');
            
            // Reload users
            await loadUsersData();
        } else {
            showMessage(data.detail || 'Failed to create user', 'error');
        }
    } catch (error) {
        console.error('Error creating user:', error);
        showMessage('Failed to create user: ' + error.message, 'error');
    }
}

// Attach form listener when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    const addUserForm = document.getElementById('addUserForm');
    if (addUserForm) {
        addUserForm.addEventListener('submit', handleAddUserSubmit);
    }
});

// ============= Edit User Modal =============
function openEditUserModal(userId) {
    const user = allUsers.find(u => u.id === userId);
    if (!user) return;
    
    document.getElementById('editUserId').value = user.id;
    document.getElementById('editUsername').value = user.username;
    document.getElementById('editEmail').value = user.email;
    document.getElementById('editFullName').value = user.full_name || '';
    document.getElementById('editRole').value = user.role.toLowerCase();
    document.getElementById('editIsActive').value = user.is_active.toString();
    
    document.getElementById('editUserModal').classList.add('show');
}

function closeEditUserModal() {
    document.getElementById('editUserModal').classList.remove('show');
}

async function handleEditUserSubmit(e) {
    e.preventDefault();
    
    const userId = document.getElementById('editUserId').value;
    const email = document.getElementById('editEmail').value.trim();
    const fullName = document.getElementById('editFullName').value.trim();
    const role = document.getElementById('editRole').value;
    const isActive = document.getElementById('editIsActive').value === 'true';
    
    try {
        const response = await fetch(`/api/v1/users/${userId}`, {
            method: 'PUT',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                email,
                full_name: fullName || null,
                role,
                is_active: isActive
            })
        });
        
        const data = await response.json();
        
        if (response.ok) {
            showMessage('User updated successfully!', 'success');
            closeEditUserModal();
            await loadUsersData();
        } else {
            showMessage(data.detail || 'Failed to update user', 'error');
        }
    } catch (error) {
        console.error('Error updating user:', error);
        showMessage('Failed to update user: ' + error.message, 'error');
    }
}

document.addEventListener('DOMContentLoaded', () => {
    const editUserForm = document.getElementById('editUserForm');
    if (editUserForm) {
        editUserForm.addEventListener('submit', handleEditUserSubmit);
    }
});

// ============= Reset Password Modal =============
function openResetPasswordModal(userId) {
    const user = allUsers.find(u => u.id === userId);
    if (!user) return;
    
    document.getElementById('resetPasswordUserId').value = user.id;
    document.getElementById('resetPasswordUsername').textContent = user.username;
    document.getElementById('resetNewPassword').value = '';
    document.getElementById('resetPasswordConfirm').value = '';
    
    document.getElementById('resetPasswordModal').classList.add('show');
}

function closeResetPasswordModal() {
    document.getElementById('resetPasswordModal').classList.remove('show');
}

async function handleResetPasswordSubmit(e) {
    e.preventDefault();
    
    const userId = document.getElementById('resetPasswordUserId').value;
    const username = document.getElementById('resetPasswordUsername').textContent;
    const newPassword = document.getElementById('resetNewPassword').value;
    const passwordConfirm = document.getElementById('resetPasswordConfirm').value;
    
    // Validate passwords match
    if (newPassword !== passwordConfirm) {
        showMessage('Passwords do not match', 'error');
        return;
    }
    
    // Validate password complexity
    if (newPassword.length < 8) {
        showMessage('Password must be at least 8 characters', 'error');
        return;
    }
    
    if (!/[^a-zA-Z0-9]/.test(newPassword)) {
        showMessage('Password must contain at least one non-alphanumeric character', 'error');
        return;
    }
    
    // Confirm action
    if (!confirm(`Are you sure you want to reset the password for user '${username}'?`)) {
        return;
    }
    
    try {
        const response = await fetch(`/api/v1/users/${userId}/reset-password`, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                new_password: newPassword
            })
        });
        
        const data = await response.json();
        
        if (response.ok) {
            showMessage(`Password reset successfully for '${username}'!`, 'success');
            closeResetPasswordModal();
        } else {
            showMessage(data.detail || 'Failed to reset password', 'error');
        }
    } catch (error) {
        console.error('Error resetting password:', error);
        showMessage('Failed to reset password: ' + error.message, 'error');
    }
}

document.addEventListener('DOMContentLoaded', () => {
    const resetPasswordForm = document.getElementById('resetPasswordForm');
    if (resetPasswordForm) {
        resetPasswordForm.addEventListener('submit', handleResetPasswordSubmit);
    }
});

// ============= Toggle User Status =============
async function toggleUserStatus(userId, newStatus) {
    const user = allUsers.find(u => u.id === userId);
    if (!user) return;
    
    const action = newStatus ? 'activate' : 'deactivate';
    const confirmMsg = `Are you sure you want to ${action} user '${user.username}'?`;
    
    if (!confirm(confirmMsg)) {
        return;
    }
    
    try {
        const response = await fetch(`/api/v1/users/${userId}`, {
            method: 'PUT',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                is_active: newStatus
            })
        });
        
        const data = await response.json();
        
        if (response.ok) {
            showMessage(`User '${user.username}' ${action}d successfully!`, 'success');
            await loadUsersData();
        } else {
            showMessage(data.detail || `Failed to ${action} user`, 'error');
        }
    } catch (error) {
        console.error(`Error ${action}ing user:`, error);
        showMessage(`Failed to ${action} user: ` + error.message, 'error');
    }
}


// Close modals when clicking outside
window.addEventListener('click', (event) => {
    const editModal = document.getElementById('editUserModal');
    const resetModal = document.getElementById('resetPasswordModal');
    
    if (event.target === editModal) {
        closeEditUserModal();
    }
    if (event.target === resetModal) {
        closeResetPasswordModal();
    }
});

/**
 * Toggle collapsible form sections
 * Shows/hides form content and changes +/- icon
 */
function toggleFormSection(header) {
    const content = header.nextElementSibling;
    const icon = header.querySelector('.toggle-icon');
    
    if (content.classList.contains('expanded')) {
        content.classList.remove('expanded');
        icon.textContent = '+';
    } else {
        content.classList.add('expanded');
        icon.textContent = '−';  // This is the minus sign (U+2212)
    }
}