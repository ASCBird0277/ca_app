/* global L */
(() => {
  'use strict';

  const state = {
    allProperties: [],
    visibleProperties: [],
    employeeMatches: [],
    regions: [],
    markers: new Map(),
    map: null,
    markerLayer: null,
    searchTimer: null,
    filters: {
      query: '',
      regions: new Set(),
      vacancy: 'all',
      unitsMin: null,
      unitsMax: null,
    },
    virtualization: {
      enabled: false,
      handler: null,
    },
    isSearching: false,
  };

  const SELECTORS = {
    searchInput: 'searchInput',
    searchSpinner: 'searchSpinner',
    searchResultsPanel: 'searchResultsPanel',
    propertyCount: 'propertyCount',
    sidebarContent: 'sidebarContent',
    propertyCards: 'propertyCards',
    emptyState: 'emptyState',
    regionFilterToggle: 'regionFilterToggle',
    regionFilterPanel: 'regionFilterPanel',
    regionFilterCount: 'regionFilterCount',
    vacancySelect: 'vacancySelect',
    unitsMin: 'unitsMin',
    unitsMax: 'unitsMax',
    clearFiltersButton: 'clearFiltersButton',
    reloadButton: 'reloadButton',
    statusBanner: 'statusBanner',
    staffDrawer: 'staffDrawer',
    drawerTitle: 'drawerTitle',
    drawerContent: 'drawerContent',
    drawerClose: 'drawerClose',
  };

  const VIRTUAL_THRESHOLD = 120;
  const CARD_HEIGHT = 148;

  document.addEventListener('DOMContentLoaded', () => {
    initializeMap();
    bindUI();
    loadInitialData();
  });

  function initializeMap() {
    state.map = L.map('map', {
      minZoom: 3,
      maxZoom: 18,
      worldCopyJump: true,
    }).setView([39.5, -98.35], 4);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap contributors',
      maxZoom: 19,
    }).addTo(state.map);
    state.markerLayer = L.layerGroup().addTo(state.map);

    state.map.on('popupopen', (event) => {
      const popupEl = event.popup.getElement();
      if (!popupEl) return;
      const button = popupEl.querySelector('.view-staff-btn');
      if (button) {
        button.addEventListener('click', () => {
          const propertyId = button.getAttribute('data-property-id');
          const propertyName = button.getAttribute('data-property-name');
          openStaffDrawer(propertyId, propertyName);
        }, { once: true });
      }
    });
  }

  function bindUI() {
    const searchInput = getElement(SELECTORS.searchInput);
    const searchSpinner = getElement(SELECTORS.searchSpinner);
    const searchContainer = searchInput.parentElement;
    document.addEventListener('click', (event) => {
      if (!searchContainer.contains(event.target)) {
        hideSearchOverlay();
      }
    });

    searchInput.addEventListener('input', () => {
      const value = searchInput.value.trim();
      state.filters.query = value;
      debounceSearch();
    });

    const vacancySelect = getElement(SELECTORS.vacancySelect);
    vacancySelect.addEventListener('change', () => {
      state.filters.vacancy = vacancySelect.value;
      performSearch();
    });

    const unitsMin = getElement(SELECTORS.unitsMin);
    const unitsMax = getElement(SELECTORS.unitsMax);
    unitsMin.addEventListener('change', () => {
      state.filters.unitsMin = parseNumber(unitsMin.value);
      performSearch();
    });
    unitsMax.addEventListener('change', () => {
      state.filters.unitsMax = parseNumber(unitsMax.value);
      performSearch();
    });

    getElement(SELECTORS.clearFiltersButton).addEventListener('click', () => {
      state.filters.regions.clear();
      state.filters.vacancy = 'all';
      state.filters.unitsMin = null;
      state.filters.unitsMax = null;
      state.filters.query = '';
      searchInput.value = '';
      vacancySelect.value = 'all';
      unitsMin.value = '';
      unitsMax.value = '';
      updateRegionSelections();
      performSearch();
    });

    getElement(SELECTORS.reloadButton).addEventListener('click', async () => {
      await reloadData();
    });

    const regionToggle = getElement(SELECTORS.regionFilterToggle);
    const regionPanel = getElement(SELECTORS.regionFilterPanel);
    regionToggle.addEventListener('click', () => {
      regionPanel.classList.toggle('hidden');
    });
    document.addEventListener('click', (event) => {
      if (!regionToggle.contains(event.target) && !regionPanel.contains(event.target)) {
        regionPanel.classList.add('hidden');
      }
    });

    getElement(SELECTORS.drawerClose).addEventListener('click', () => {
      closeStaffDrawer();
    });

    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') {
        closeStaffDrawer();
      }
    });

    state.ui = { searchSpinner };
  }

  function debounceSearch() {
    if (state.searchTimer) {
      window.clearTimeout(state.searchTimer);
    }
    state.searchTimer = window.setTimeout(() => {
      performSearch();
    }, 250);
  }

  async function loadInitialData() {
    showStatus('Loading properties...', 'info');
    try {
      const response = await fetch('/api/properties');
      if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
      }
      const data = await response.json();
      state.allProperties = data;
      state.visibleProperties = data;
      state.employeeMatches = [];
      updatePropertyCount();
      updateRegions();
      renderSidebar();
      renderMarkers();
      renderSearchOverlay();
      showStatus('Properties loaded', 'success', 1800);
    } catch (error) {
      state.employeeMatches = [];
      hideSearchOverlay();
      state.employeeMatches = [];
      hideSearchOverlay();
      state.employeeMatches = [];
      hideSearchOverlay();
      console.error(error);
      showStatus('Failed to load properties', 'error', 4000);
    }
  }

  async function reloadData() {
    showStatus('Reloading data...', 'info');
    try {
      const response = await fetch('/api/reload', {
        method: 'POST',
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.message || 'Reload failed');
      }
      await loadInitialData();
    } catch (error) {
      console.error(error);
      showStatus(`Reload failed: ${error.message}`, 'error', 4000);
    }
  }

  function updatePropertyCount() {
    const countElement = getElement(SELECTORS.propertyCount);
    countElement.textContent = `${state.visibleProperties.length}`;
  }

  function updateRegions() {
    const regions = Array.from(
      new Set(
        state.allProperties
          .map((property) => property.region)
          .filter((region) => region)
      )
    ).sort((a, b) => a.localeCompare(b));
    state.regions = regions;

    const panel = getElement(SELECTORS.regionFilterPanel);
    panel.innerHTML = '';
    if (!regions.length) {
      panel.innerHTML = '<p class="text-xs text-slate-500">No regions available.</p>';
      return;
    }
    const fragment = document.createDocumentFragment();
    regions.forEach((region) => {
      const id = `region-${slugify(region)}`;
      const wrapper = document.createElement('label');
      wrapper.className = 'flex items-center gap-2 py-1 text-slate-700';
      wrapper.innerHTML = `
        <input type="checkbox" class="rounded border-slate-300 text-indigo-600 focus:ring-indigo-500" id="${id}" value="${region}">
        <span>${region}</span>
      `;
      const input = wrapper.querySelector('input');
      input.checked = state.filters.regions.has(region);
      input.addEventListener('change', () => {
        if (input.checked) {
          state.filters.regions.add(region);
        } else {
          state.filters.regions.delete(region);
        }
        updateRegionSelections();
        performSearch();
      });
      fragment.appendChild(wrapper);
    });
    panel.appendChild(fragment);
    updateRegionSelections();
  }

  function updateRegionSelections() {
    const countElement = getElement(SELECTORS.regionFilterCount);
    const selectedCount = state.filters.regions.size;
    if (selectedCount > 0) {
      countElement.textContent = `${selectedCount}`;
      countElement.classList.remove('hidden');
    } else {
      countElement.textContent = '';
      countElement.classList.add('hidden');
    }
    const panel = getElement(SELECTORS.regionFilterPanel);
    panel.querySelectorAll('input[type="checkbox"]').forEach((checkbox) => {
      checkbox.checked = state.filters.regions.has(checkbox.value);
    });
  }

  async function performSearch() {
    state.isSearching = true;
    toggleSearchSpinner(true);
    try {
      const params = new URLSearchParams();
      if (state.filters.query.trim()) {
        params.append('q', state.filters.query.trim());
      }
      if (state.filters.vacancy !== 'all') {
        params.append('vacancy', state.filters.vacancy);
      }
      if (state.filters.unitsMin != null) {
        params.append('unitsMin', `${state.filters.unitsMin}`);
      }
      if (state.filters.unitsMax != null) {
        params.append('unitsMax', `${state.filters.unitsMax}`);
      }
      state.filters.regions.forEach((region) => params.append('region', region));

      let response;
      if (!params.toString()) {
        response = await fetch('/api/properties');
      } else {
        response = await fetch(`/api/search?${params.toString()}`);
      }
      if (!response.ok) {
        throw new Error('Search request failed');
      }
      const payload = await response.json();
      if (Array.isArray(payload)) {
        state.visibleProperties = payload;
        state.employeeMatches = [];
      } else {
        state.visibleProperties = payload.properties || [];
        state.employeeMatches = (payload.employeeMatches || []).slice(0, 25);
      }
      if (!state.filters.query.trim()) {
        state.employeeMatches = [];
      }
      updatePropertyCount();
      renderSidebar();
      renderMarkers();
      toggleEmptyState();
      renderSearchOverlay();
    } catch (error) {
      state.employeeMatches = [];
      hideSearchOverlay();
      console.error(error);
      showStatus('Search failed', 'error', 3000);
    } finally {
      toggleSearchSpinner(false);
      state.isSearching = false;
    }
  }

  function renderSidebar() {
    const scroller = getElement(SELECTORS.sidebarContent);
    scroller.classList.remove('virtualized');
    scroller.innerHTML = '';

    const container = document.createElement('div');
    container.id = SELECTORS.propertyCards;
    container.className = 'space-y-3 p-4';
    scroller.appendChild(container);

    ensureEmptyState(scroller);

    if (state.visibleProperties.length > VIRTUAL_THRESHOLD) {
      scroller.classList.add('virtualized');
      setupVirtualizedList(scroller, state.visibleProperties);
      return;
    }

    container.innerHTML = '';
    state.visibleProperties.forEach((property) => {
      container.appendChild(createPropertyCard(property));
    });
  }

  function ensureEmptyState(scroller) {
    let emptyState = document.getElementById(SELECTORS.emptyState);
    if (!emptyState) {
      emptyState = document.createElement('div');
      emptyState.id = SELECTORS.emptyState;
      emptyState.className = 'hidden px-4 py-6 text-sm text-slate-300';
      emptyState.textContent = 'No properties match your filters.';
      scroller.appendChild(emptyState);
      return emptyState;
    }
    if (emptyState.parentElement !== scroller) {
      emptyState.remove();
      scroller.appendChild(emptyState);
    }
    return emptyState;
  }

  function setupVirtualizedList(scroller, data) {
    const spacer = document.createElement('div');
    spacer.className = 'virtual-spacer';
    spacer.style.height = `${data.length * CARD_HEIGHT}px`;

    const content = document.createElement('div');
    content.className = 'virtual-content space-y-3 px-4';
    scroller.innerHTML = '';
    scroller.appendChild(spacer);
    scroller.appendChild(content);
    ensureEmptyState(scroller);

    const renderChunk = () => {
      const scrollTop = scroller.scrollTop;
      const viewport = scroller.clientHeight || 1;
      const startIndex = Math.max(0, Math.floor(scrollTop / CARD_HEIGHT) - 5);
      const endIndex = Math.min(
        data.length,
        startIndex + Math.ceil(viewport / CARD_HEIGHT) + 10,
      );
      const fragment = document.createDocumentFragment();
      for (let index = startIndex; index < endIndex; index += 1) {
        fragment.appendChild(createPropertyCard(data[index]));
      }
      content.innerHTML = '';
      content.style.transform = `translateY(${startIndex * CARD_HEIGHT}px)`;
      content.appendChild(fragment);
    };

    if (state.virtualization.handler) {
      scroller.removeEventListener('scroll', state.virtualization.handler);
    }
    state.virtualization.handler = () => window.requestAnimationFrame(renderChunk);
    scroller.addEventListener('scroll', state.virtualization.handler);
    renderChunk();
  }

  function renderMarkers() {
    state.markerLayer.clearLayers();
    state.markers.clear();
    if (!state.visibleProperties.length) {
      return;
    }
    const greenIcon = createMarkerIcon('green');
    const yellowIcon = createMarkerIcon('yellow');

    state.visibleProperties.forEach((property) => {
      if (!property.hasCoordinates) {
        return;
      }
      const marker = L.marker([property.latitude, property.longitude], {
        icon: property.hasVacancy ? yellowIcon : greenIcon,
        title: property.property,
      });
      marker.bindTooltip(property.tooltip, { direction: 'top', offset: [0, -12] });
      marker.bindPopup(property.popupHtml, { maxWidth: 320 });
      marker.addTo(state.markerLayer);
      state.markers.set(property.propertyId, marker);
    });
  }

  function renderSearchOverlay() {
    const panel = getElement(SELECTORS.searchResultsPanel);
    const query = state.filters.query.trim();
    if (!panel) {
      return;
    }
    if (!query) {
      hideSearchOverlay();
      return;
    }

    const employeeMatches = state.employeeMatches || [];
    const propertyMatches = state.visibleProperties.slice(0, 8);
    const hasMatches = employeeMatches.length > 0 || propertyMatches.length > 0;

    panel.innerHTML = '';
    if (!hasMatches) {
      const empty = document.createElement('div');
      empty.className = 'search-results-empty';
      empty.textContent = 'No matches found';
      panel.appendChild(empty);
      panel.classList.remove('hidden');
      return;
    }

    const buildButton = (label, action, propertyId, propertyName) => {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = action === 'center' ? 'result-button result-button-secondary' : 'result-button';
      button.dataset.action = action;
      if (propertyId) {
        button.dataset.propertyId = propertyId;
      }
      if (propertyName) {
        button.dataset.propertyName = propertyName;
      }
      button.textContent = label;
      if (action === 'open-staff') {
        button.addEventListener('click', () => {
          openStaffDrawer(propertyId, propertyName);
        });
      }
      if (action === 'center') {
        button.addEventListener('click', () => {
          centerOnProperty(propertyId);
        });
      }
      return button;
    };

    const wrapper = document.createElement('div');
    wrapper.className = 'search-results-wrapper';
    panel.appendChild(wrapper);

    if (employeeMatches.length) {
      const section = document.createElement('div');
      section.className = 'search-results-section';
      const title = document.createElement('p');
      title.className = 'search-results-title';
      title.textContent = 'Matching staff (' + employeeMatches.length + ')';
      section.appendChild(title);

      const list = document.createElement('ul');
      list.className = 'search-results-list';

      employeeMatches.forEach((match) => {
        const propertyId = match.propertyId || '';
        const propertyName = match.property || 'Property n/a';
        const item = document.createElement('li');
        item.className = 'search-result-item';
        if (propertyId) {
          item.dataset.propertyId = propertyId;
        }

        const left = document.createElement('div');
        left.className = 'result-col';
        const primary = document.createElement('p');
        primary.className = 'result-primary';
        primary.textContent = match.employeeName || 'Name n/a';
        const secondary = document.createElement('p');
        secondary.className = 'result-secondary';
        secondary.textContent = match.jobTitle || 'Role n/a';
        left.append(primary, secondary);

        const right = document.createElement('div');
        right.className = 'result-col result-col-right';
        const tertiary = document.createElement('p');
        tertiary.className = 'result-tertiary';
        tertiary.textContent = propertyName;
        const actions = document.createElement('div');
        actions.className = 'result-actions';
        actions.append(
          buildButton('View staff', 'open-staff', propertyId, propertyName),
          buildButton('Center', 'center', propertyId, propertyName),
        );
        right.append(tertiary, actions);

        item.append(left, right);
        list.appendChild(item);
      });

      section.appendChild(list);
      wrapper.appendChild(section);
    }

    if (propertyMatches.length) {
      const section = document.createElement('div');
      section.className = 'search-results-section';
      const title = document.createElement('p');
      title.className = 'search-results-title';
      title.textContent = 'Matching properties (' + state.visibleProperties.length + ')';
      section.appendChild(title);

      const list = document.createElement('ul');
      list.className = 'search-results-list';

      propertyMatches.forEach((property) => {
        const propertyId = property.propertyId;
        const item = document.createElement('li');
        item.className = 'search-result-item';
        if (propertyId) {
          item.dataset.propertyId = propertyId;
        }

        const left = document.createElement('div');
        left.className = 'result-col';
        const primary = document.createElement('p');
        primary.className = 'result-primary';
        primary.textContent = property.property || 'Property';
        const secondary = document.createElement('p');
        secondary.className = 'result-secondary';
        secondary.textContent = [property.city, property.state].filter(Boolean).join(', ') || 'Location n/a';
        left.append(primary, secondary);

        const right = document.createElement('div');
        right.className = 'result-col result-col-right';
        const status = document.createElement('p');
        status.className = 'result-tertiary ' + (property.hasVacancy ? 'text-amber-500' : 'text-emerald-500');
        status.textContent = property.hasVacancy ? 'Vacancy' : 'Fully staffed';
        const actions = document.createElement('div');
        actions.className = 'result-actions';
        actions.append(
          buildButton('View staff', 'open-staff', propertyId, property.property),
          buildButton('Center', 'center', propertyId, property.property),
        );
        right.append(status, actions);

        item.append(left, right);
        list.appendChild(item);
      });

      section.appendChild(list);
      wrapper.appendChild(section);
    }

    panel.classList.remove('hidden');
  }

  function hideSearchOverlay() {
    const panel = getElement(SELECTORS.searchResultsPanel);
    panel.classList.add('hidden');
    panel.innerHTML = '';
  }

  function createMarkerIcon(color) {
    return L.divIcon({
      className: `custom-marker marker-${color}`,
      html: '<span></span>',
      iconSize: [30, 42],
      iconAnchor: [15, 42],
      popupAnchor: [0, -36],
    });
  }

  function createPropertyCard(property) {
    const card = document.createElement('article');
    card.className = 'property-card rounded-lg border border-slate-800 bg-slate-900/60 p-4 shadow-md transition hover:border-indigo-400';
    card.setAttribute('data-property-id', property.propertyId);

    const vacancyClass = property.hasVacancy
      ? 'bg-amber-300/90 text-slate-900'
      : 'bg-emerald-300/90 text-slate-900';
    const vacancyLabel = property.hasVacancy ? 'Vacancy' : 'Fully staffed';
    const vacancyDetails = property.hasVacancy
      ? `${property.vacantPositions} open`
      : 'All positions filled';
    const location = [property.city, property.state].filter(Boolean).join(', ');
    const addressLine = [property.address, property.city, property.state, property.zip].filter(Boolean).join(', ');
    const unitsText = property.units != null ? `${property.units} units` : 'Units n/a';
    const metaEntries = [unitsText];
    if (property.region) {
      metaEntries.push(`Region: ${property.region}`);
    }
    const metaHtml = metaEntries.map((entry) => `<span>${entry}</span>`).join('');
    const noLocationBadge = property.hasCoordinates
      ? ''
      : '<span class="badge badge-warning">No map location</span>';

    const formatStaffText = (staff) => {
      if (!staff) {
        return 'Not assigned';
      }
      if (staff.isVacant) {
        return 'Vacant';
      }
      return staff.employeeName || 'Not assigned';
    };

    const regionalManager = formatStaffText(property.regionalManager);
    const regionalMaintenance = formatStaffText(property.regionalMaintenanceSupervisor);

    card.innerHTML = `
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0">
          <h3 class="truncate text-base font-semibold text-white">${property.property}</h3>
          <p class="text-xs text-slate-300">${location || 'Location unavailable'}</p>
        </div>
        <span class="status-chip ${vacancyClass}">${vacancyLabel}</span>
      </div>
      <div class="mt-3 space-y-2 text-xs text-slate-300">
        <p>${addressLine || 'Address n/a'}</p>
        <div class="flex flex-wrap gap-3 text-[11px] uppercase tracking-wide text-slate-400">${metaHtml}</div>
        <p>${vacancyDetails}</p>
        ${noLocationBadge}
      </div>
      <div class="mt-3 space-y-1 text-xs text-slate-200">
        <p><span class="font-semibold text-slate-100">Regional Manager:</span> ${regionalManager}</p>
        <p><span class="font-semibold text-slate-100">Regional Maintenance:</span> ${regionalMaintenance}</p>
      </div>
      <div class="mt-4 flex items-center justify-between gap-2">
        <button type="button" class="btn-secondary text-xs" data-action="center">
          Center on map
        </button>
        <button type="button" class="btn-primary text-xs" data-action="staff">
          View staff
        </button>
      </div>
    `;

    card.querySelector('[data-action="center"]').addEventListener('click', () => {
      centerOnProperty(property.propertyId);
    });
    card.querySelector('[data-action="staff"]').addEventListener('click', () => {
      openStaffDrawer(property.propertyId, property.property);
    });

    return card;
  }


  function centerOnProperty(propertyId) {
    const property = state.visibleProperties.find((item) => item.propertyId === propertyId);
    if (!property) return;
    if (!property.hasCoordinates) {
      showStatus('No coordinates for this property', 'warning', 2000);
      return;
    }
    const marker = state.markers.get(propertyId);
    if (!marker) return;
    marker.openPopup();
    state.map.flyTo(marker.getLatLng(), Math.max(state.map.getZoom(), 14), {
      duration: 0.8,
    });
  }

  async function openStaffDrawer(propertyId, propertyName) {
    const drawer = getElement(SELECTORS.staffDrawer);
    const title = getElement(SELECTORS.drawerTitle);
    const content = getElement(SELECTORS.drawerContent);

    drawer.classList.remove('hidden');
    title.textContent = propertyName || 'Staff';
    content.innerHTML = '<p class="text-sm text-slate-500">Loading...</p>';

    try {
      const response = await fetch(`/api/employees?property=${encodeURIComponent(propertyId)}`);
      if (!response.ok) {
        throw new Error('Failed to load staff');
      }
      const data = await response.json();
      renderDrawerContent(data);
    } catch (error) {
      console.error(error);
      content.innerHTML = '<p class="text-sm text-rose-600">Unable to load staff details.</p>';
    }
  }

  function renderDrawerContent(data) {
    const content = getElement(SELECTORS.drawerContent);
    const employees = data.employees || [];
    if (!employees.length) {
      content.innerHTML = '<p class="text-sm text-slate-600">No positions recorded for this property.</p>';
      return;
    }
    const fragment = document.createElement('div');
    fragment.className = 'space-y-3';

    employees.forEach((employee) => {
      const card = document.createElement('div');
      card.className = 'rounded-lg border border-slate-200 bg-white p-3 shadow-sm';
      const name = employee.employeeName || 'Vacant position';
      const vacancyClass = employee.isVacant ? 'text-amber-600' : 'text-emerald-600';
      card.innerHTML = `
        <div class="flex items-start justify-between">
          <div>
            <p class="text-sm font-semibold text-slate-900">${name}</p>
            <p class="text-xs text-slate-500">${employee.jobTitle || 'Role n/a'}</p>
          </div>
          <span class="text-xs font-semibold ${vacancyClass}">
            ${employee.isVacant ? 'Vacant' : 'Filled'}
          </span>
        </div>
        <div class="mt-2 space-y-1 text-xs text-slate-600">
          ${employee.email ? `<p>Email: <a href="mailto:${employee.email}" class="text-indigo-600 underline">${employee.email}</a></p>` : ''}
          ${employee.phone ? `<p>Phone: <a href="tel:${employee.phone}" class="text-indigo-600 underline">${employee.phone}</a></p>` : ''}
        </div>
      `;
      fragment.appendChild(card);
    });
    content.innerHTML = '';
    content.appendChild(fragment);
  }

  function closeStaffDrawer() {
    const drawer = getElement(SELECTORS.staffDrawer);
    drawer.classList.add('hidden');
  }

  function renderMarkersEmptyState() {
    if (!state.visibleProperties.length) {
      state.markerLayer.clearLayers();
    }
  }

  function toggleEmptyState() {
    const scroller = getElement(SELECTORS.sidebarContent);
    const emptyState = ensureEmptyState(scroller);
    if (!state.visibleProperties.length) {
      emptyState.classList.remove('hidden');
      renderMarkersEmptyState();
    } else {
      emptyState.classList.add('hidden');
    }
  }

  function toggleSearchSpinner(show) {
    const spinner = getElement(SELECTORS.searchSpinner);
    if (show) {
      spinner.classList.remove('hidden');
    } else {
      spinner.classList.add('hidden');
    }
  }

  function showStatus(message, type = 'info', duration = 2500) {
    const banner = getElement(SELECTORS.statusBanner);
    banner.textContent = message;
    banner.classList.remove('hidden', 'status-info', 'status-success', 'status-error', 'status-warning');
    banner.classList.add(`status-${type}`);
    if (duration > 0) {
      window.setTimeout(() => {
        banner.classList.add('hidden');
      }, duration);
    }
  }

  function toggleStatusPersistent(show) {
    const banner = getElement(SELECTORS.statusBanner);
    if (show) {
      banner.classList.remove('hidden');
    } else {
      banner.classList.add('hidden');
    }
  }

  function parseNumber(value) {
    if (!value && value !== 0) {
      return null;
    }
    const parsed = Number(value);
    if (Number.isNaN(parsed)) {
      return null;
    }
    return parsed;
  }

  function getElement(id) {
    const element = document.getElementById(id);
    if (!element) {
      throw new Error(`Missing element #${id}`);
    }
    return element;
  }

  function slugify(value) {
    return String(value || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
  }
})();
