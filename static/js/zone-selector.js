(function () {
  const radiusOptions = {
    "1km": { label: "1 km", radiusKm: 1 },
    "2km": { label: "2 km", radiusKm: 2 },
    "5km": { label: "5 km", radiusKm: 5 },
    "10km": { label: "10 km", radiusKm: 10 },
    "30km": { label: "30 km", radiusKm: 30 },
    "50km": { label: "50 km", radiusKm: 50 },
    "100km": { label: "100 km", radiusKm: 100 },
    "200km": { label: "200 km", radiusKm: 200 },
    "200_plus": { label: "+200 km", radiusKm: null },
  };
  const radiusBucketsOrdered = [
    { bucket: "1km", radiusKm: 1 },
    { bucket: "2km", radiusKm: 2 },
    { bucket: "5km", radiusKm: 5 },
    { bucket: "10km", radiusKm: 10 },
    { bucket: "30km", radiusKm: 30 },
    { bucket: "50km", radiusKm: 50 },
    { bucket: "100km", radiusKm: 100 },
    { bucket: "200km", radiusKm: 200 },
    { bucket: "200_plus", radiusKm: 220 },
  ];

  const defaultZone = () => ({
    mode: "radius_from_point",
    label: "",
    center: { lat: null, lon: null },
    radius_km: 10,
    radius_bucket: "10km",
    source: "",
    raw_query: "",
    admin_level: "",
    bbox: null,
    geojson: null,
  });
  const defaultSpainBounds = [
    [36.0, -9.9],
    [44.2, 4.2],
  ];

  const parseJson = (text, fallback) => {
    try {
      return JSON.parse(text);
    } catch (_) {
      return fallback;
    }
  };

  const debounce = (fn, delay) => {
    let timer = null;
    return (...args) => {
      clearTimeout(timer);
      timer = setTimeout(() => fn(...args), delay);
    };
  };

  const normalizeRadius = (bucket) => radiusOptions[bucket] || radiusOptions["10km"];
  const truncateLabel = (value, maxLength = 20) => {
    const text = String(value || "").trim();
    if (!text) return "Área administrativa";
    return text.length > maxLength ? `${text.slice(0, maxLength).trimEnd()}...` : text;
  };
  const zoneSummaryLabel = (payload, placeholder) => {
    if (!payload || (!payload.label && payload.center?.lat == null && !payload.geojson)) {
      return placeholder || "Cualquier ubicación";
    }
    if (payload.mode === "radius_from_point") {
      const radius = normalizeRadius(payload.radius_bucket || "10km");
      if (payload.label === "Área personalizada" || payload.raw_query === "Área personalizada") {
        return `Área personalizada · ${radius.label}`;
      }
      return payload.label || `Área personalizada · ${radius.label}`;
    }
    return payload.label || placeholder || "Cualquier ubicación";
  };
  const compactZoneForTransport = (payload) => {
    if (!payload || typeof payload !== "object") return payload;
    const clone = JSON.parse(JSON.stringify(payload));
    if (clone.mode === "area") {
      const derived = deriveRadiusZoneFromArea(clone);
      return {
        mode: "radius_from_point",
        label: clone.label || "",
        center: derived.center,
        radius_km: derived.radius_km,
        radius_bucket: derived.radius_bucket,
        source: clone.source || "",
        raw_query: clone.raw_query || "",
        admin_level: clone.admin_level || "",
        bbox: null,
        geojson: null,
      };
    }
    if (clone.geojson) clone.geojson = null;
    return clone;
  };
  const toRadians = (value) => (value * Math.PI) / 180;
  const haversineKm = (lat1, lon1, lat2, lon2) => {
    const earthRadiusKm = 6371;
    const dLat = toRadians(lat2 - lat1);
    const dLon = toRadians(lon2 - lon1);
    const a =
      Math.sin(dLat / 2) ** 2 +
      Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * Math.sin(dLon / 2) ** 2;
    const c = 2 * Math.asin(Math.sqrt(a));
    return earthRadiusKm * c;
  };
  const nearestRadiusBucket = (radiusKm) => {
    if (!Number.isFinite(radiusKm) || radiusKm <= 0) return { bucket: "10km", radiusKm: 10 };
    let best = radiusBucketsOrdered[0];
    let bestDistance = Math.abs(radiusKm - best.radiusKm);
    radiusBucketsOrdered.forEach((item) => {
      const distance = Math.abs(radiusKm - item.radiusKm);
      if (distance < bestDistance) {
        best = item;
        bestDistance = distance;
      }
    });
    return best;
  };
  const deriveRadiusZoneFromArea = (areaZone) => {
    const bbox = Array.isArray(areaZone?.bbox) && areaZone.bbox.length === 4 ? areaZone.bbox : null;
    const center = areaZone?.center || {};
    let lat = Number(center.lat);
    let lon = Number(center.lon);
    if ((!Number.isFinite(lat) || !Number.isFinite(lon)) && bbox) {
      lon = (Number(bbox[0]) + Number(bbox[2])) / 2;
      lat = (Number(bbox[1]) + Number(bbox[3])) / 2;
    }
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      return defaultZone();
    }

    let estimatedRadiusKm = 10;
    if (bbox) {
      const corners = [
        [Number(bbox[1]), Number(bbox[0])],
        [Number(bbox[1]), Number(bbox[2])],
        [Number(bbox[3]), Number(bbox[0])],
        [Number(bbox[3]), Number(bbox[2])],
      ];
      estimatedRadiusKm = corners.reduce((maxDistance, [cornerLat, cornerLon]) => {
        return Math.max(maxDistance, haversineKm(lat, lon, cornerLat, cornerLon));
      }, 0);
    }
    const nearest = nearestRadiusBucket(estimatedRadiusKm);
    return {
      mode: "radius_from_point",
      label: "",
      center: { lat, lon },
      radius_bucket: nearest.bucket,
      radius_km: nearest.bucket === "200_plus" ? null : nearest.radiusKm,
      source: areaZone?.source || "autocomplete",
      raw_query: "",
      admin_level: "",
      bbox: null,
      geojson: null,
    };
  };
  const normalizeGeoName = (value) =>
    String(value || "")
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/-/g, " ")
      .replace(/\s+/g, " ");

  const spainAdminAliases = new Set([
    "espana", "spain", "catalunya", "cataluna", "catalonia", "andalucia", "aragon",
    "asturias", "principado de asturias", "illes balears", "islas baleares", "canarias",
    "cantabria", "castilla la mancha", "castilla y leon", "comunidad de madrid", "madrid",
    "comunidad valenciana", "comunitat valenciana", "extremadura", "galicia", "la rioja",
    "navarra", "comunidad foral de navarra", "murcia", "region de murcia", "pais vasco",
    "euskadi", "ceuta", "melilla", "albacete", "alicante", "almeria", "avila", "badajoz",
    "barcelona", "burgos", "caceres", "cadiz", "castellon", "ciudad real", "cordoba",
    "cuenca", "girona", "gerona", "granada", "guadalajara", "gipuzkoa", "guipuzcoa",
    "huelva", "huesca", "jaen", "leon", "lleida", "lerida", "lugo", "malaga", "ourense",
    "orense", "palencia", "pontevedra", "salamanca", "segovia", "sevilla", "soria",
    "tarragona", "teruel", "toledo", "valencia", "valladolid", "bizkaia", "vizcaya",
    "zamora", "zaragoza", "a coruna", "alava", "araba"
  ]);
  const isSpanishAdminQuery = (query) => {
    const normalized = normalizeGeoName(query);
    return normalized.startsWith("provincia de ")
      || normalized.startsWith("comunidad de ")
      || normalized.startsWith("comunitat ")
      || normalized.startsWith("comunidad foral de ")
      || spainAdminAliases.has(normalized);
  };

  const initSelector = (root) => {
    if (!window.L) return;
    const hiddenJson = root.querySelector("[data-zone-json]");
    const hiddenLabel = root.querySelector("[data-zone-label]");
    const searchInput = root.querySelector("[data-zone-search]");
    const clearButton = root.querySelector("[data-zone-clear]");
    const resultsBox = root.querySelector("[data-zone-results]");
    const mapContainer = root.querySelector("[data-zone-map]");
    const mapPanel = root.querySelector("[data-zone-map-panel]");
    const toggleMapButton = root.querySelector("[data-zone-toggle-map]");
    const summaryTriggers = root.querySelectorAll("[data-zone-open-modal]");
    const summaryText = root.querySelector("[data-zone-summary-text]");
    const summaryClearButton = root.querySelector("[data-zone-summary-clear]");
    const modal = root.querySelector("[data-zone-modal]");
    const modalCloseButtons = root.querySelectorAll("[data-zone-close-modal]");
    const modalApplyButton = root.querySelector("[data-zone-apply-modal]");
    const modalClearButton = root.querySelector("[data-zone-modal-clear]");
    const radiusPanel = root.querySelector("[data-zone-radius-panel]");
    const modeButtons = root.querySelectorAll("[data-zone-mode]");
    const radiusHint = root.querySelector("[data-zone-radius-hint]");
    const radiusInputs = root.querySelectorAll("[data-zone-radius-option]");
    let zone = Object.assign(defaultZone(), parseJson(root.dataset.currentZone || "{}", {}));
    let appliedZone = JSON.parse(JSON.stringify(zone));
    let adminZoneCache = zone.mode === "area" ? JSON.parse(JSON.stringify(zone)) : null;
    let manualZoneCache = zone.mode !== "area" ? JSON.parse(JSON.stringify(zone)) : null;
    let preferredMode = zone.mode === "area" ? "area" : "radius";
    const autoSubmitClear = root.dataset.autoSubmitClear === "true";
    const countryBias = (root.dataset.countryBias || "").trim();
    const modalMode = root.dataset.modalMode === "true";

    const cloneZone = (value) => JSON.parse(JSON.stringify(value || defaultZone()));
    const restoreCachesFromZone = (sourceZone) => {
      if (sourceZone?.mode === "area") {
        adminZoneCache = cloneZone(sourceZone);
        manualZoneCache = null;
        preferredMode = "area";
      } else {
        manualZoneCache = cloneZone(sourceZone);
        adminZoneCache = null;
        preferredMode = "radius";
      }
    };

    const getAppliedZone = () => (modalMode ? appliedZone : zone);
    const isModalOpen = () => Boolean(modalMode && modal && !modal.hidden);

    const map = L.map(mapContainer, { scrollWheelZoom: false }).setView([40.2, -3.7], 6);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "&copy; OpenStreetMap contributors",
      maxZoom: 19,
    }).addTo(map);

    let marker = null;
    let circle = null;
    let geoLayer = null;
    let suppressNextFocusOpen = false;
    const collapsibleMap = root.dataset.collapsibleMap === "true";

    const hideResults = () => {
      resultsBox.hidden = true;
    };

    const fitDefaultView = () => {
      map.fitBounds(defaultSpainBounds, { padding: [18, 18] });
    };

    const hideShapeLayers = () => {
      if (marker) {
        map.removeLayer(marker);
        marker = null;
      }
      if (circle) {
        map.removeLayer(circle);
        circle = null;
      }
      if (geoLayer) {
        map.removeLayer(geoLayer);
        geoLayer = null;
      }
    };

    const renderResults = (items) => {
      resultsBox.innerHTML = "";
      if (!items.length) {
        hideResults();
        return;
      }
      items.forEach((item) => {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "zone-result-item";
        button.textContent = item.label;
        button.addEventListener("click", () => {
          applySelection({
            forceMode: item.mode || "radius_from_point",
            mode: item.mode || "radius_from_point",
            label: item.label,
            center: { lat: item.lat, lon: item.lon },
            source: item.source || "autocomplete",
            raw_query: item.raw_query || searchInput.value.trim(),
            admin_level: item.admin_level || "",
            bbox: item.bbox || null,
            geojson: item.geojson || null,
          });
          hideResults();
        });
        resultsBox.appendChild(button);
      });
      resultsBox.hidden = false;
    };

    const syncModeUi = () => {
      const isArea = preferredMode === "area";
      if (radiusPanel) {
        radiusPanel.hidden = !!isArea;
      }
      if (radiusHint) {
        radiusHint.hidden = isArea;
      }
      const areaButton = root.querySelector('[data-zone-mode="area"]');
      if (areaButton) {
        const areaLabel = adminZoneCache?.label || (zone.mode === "area" ? zone.label : "");
        areaButton.textContent = truncateLabel(areaLabel);
      }
      modeButtons.forEach((button) => {
        button.classList.toggle("is-active", button.dataset.zoneMode === preferredMode);
      });
    };

    const syncHiddenFields = () => {
      const sourceZone = getAppliedZone();
      hiddenJson.value = JSON.stringify(modalMode ? compactZoneForTransport(sourceZone) : sourceZone);
      hiddenLabel.value = sourceZone.label || "";
      if (searchInput && document.activeElement !== searchInput) {
        searchInput.value = zone.label || "";
      }
      if (clearButton) {
        clearButton.hidden = !(searchInput && searchInput.value.trim());
      }
      if (summaryText) {
        summaryText.textContent = zoneSummaryLabel(sourceZone, searchInput?.placeholder);
      }
      if (summaryClearButton) {
        summaryClearButton.hidden = !(sourceZone?.label || (sourceZone?.center && sourceZone.center.lat != null));
      }
      syncModeUi();
    };

    const openModal = () => {
      if (!modalMode || !modal) return;
      zone = cloneZone(appliedZone);
      restoreCachesFromZone(zone);
      modal.hidden = false;
      document.body.classList.add("has-zone-modal");
      const needsAreaResolution = zone.mode === "area" && !zone.geojson && !zone.bbox && zone.label;
      if (needsAreaResolution) {
        syncHiddenFields();
        fitDefaultView();
        autoResolveInitialLabel();
      } else if (zone.mode === "area" || (zone.center && zone.center.lat != null && zone.center.lon != null)) {
        drawZone();
      } else {
        syncHiddenFields();
        fitDefaultView();
      }
      window.setTimeout(() => {
        map.invalidateSize();
        if (needsAreaResolution) {
          fitDefaultView();
          autoResolveInitialLabel();
        } else if (zone.mode === "area" || (zone.center && zone.center.lat != null && zone.center.lon != null)) {
          drawZone();
        } else {
          fitDefaultView();
        }
        searchInput?.focus();
      }, 60);
    };

    const closeModal = (discardChanges = true) => {
      if (!modalMode || !modal) return;
      if (discardChanges) {
        zone = cloneZone(appliedZone);
        restoreCachesFromZone(zone);
        drawZone();
      }
      modal.hidden = true;
      document.body.classList.remove("has-zone-modal");
      hideResults();
    };

    const clearZoneState = (submitAfterClear = false) => {
      zone = defaultZone();
      appliedZone = modalMode ? defaultZone() : zone;
      preferredMode = "radius";
      adminZoneCache = null;
      manualZoneCache = null;
      if (searchInput) searchInput.value = "";
      resultsBox.innerHTML = "";
      hideResults();
      clearButton.hidden = true;
      hiddenLabel.value = "";
      drawZone();
      suppressNextFocusOpen = true;
      searchInput?.blur();
      if (summaryText) {
        summaryText.textContent = zoneSummaryLabel(appliedZone, searchInput?.placeholder);
      }
      if (summaryClearButton) {
        summaryClearButton.hidden = true;
      }
      if (submitAfterClear && autoSubmitClear) {
        const form = root.closest("form");
        if (form) {
          window.setTimeout(() => form.requestSubmit(), 0);
        }
      }
    };

    const drawZone = () => {
      hideShapeLayers();
      const center = zone.center || {};
      const hasCenter = Number.isFinite(Number(center.lat)) && Number.isFinite(Number(center.lon));
      const fitCurrentZone = () => {
        if (geoLayer) {
          const bounds = geoLayer.getBounds();
          if (bounds && bounds.isValid()) {
            map.fitBounds(bounds.pad(0.15));
            return;
          }
        }
        if (hasCenter) {
          if (zone.mode === "area") {
            map.setView([Number(center.lat), Number(center.lon)], 8);
            return;
          }
          if (circle) {
            const bounds = circle.getBounds();
            if (bounds && bounds.isValid()) {
              map.fitBounds(bounds.pad(0.15));
              return;
            }
          }
          map.setView([Number(center.lat), Number(center.lon)], 10);
        }
      };

      if (!hasCenter) {
        syncHiddenFields();
        fitDefaultView();
        return;
      }
      const latLng = [Number(center.lat), Number(center.lon)];
      marker = L.marker(latLng).addTo(map);
      const radius = normalizeRadius(zone.radius_bucket);
      const radiusMeters = (radius.radiusKm == null ? 220 : radius.radiusKm) * 1000;
      circle = L.circle(latLng, {
        radius: radiusMeters,
        color: "#0f766e",
        fillColor: "rgba(15, 118, 110, 0.18)",
        fillOpacity: 0.35,
        weight: 2,
      }).addTo(map);
      syncHiddenFields();
      fitCurrentZone();
      window.setTimeout(() => {
        map.invalidateSize();
        fitCurrentZone();
      }, 60);
    };

    const applySelection = (selection) => {
      const requestedMode = selection.forceMode || selection.mode || "radius_from_point";
      const baseZone = {
        ...zone,
        mode: requestedMode,
        label: selection.label || "Punto seleccionado en el mapa",
        center: selection.center || zone.center,
        source: selection.source || zone.source || "map_click",
        raw_query: selection.raw_query || zone.raw_query || searchInput.value.trim(),
        admin_level: selection.admin_level || "",
        bbox: selection.bbox || null,
        geojson: selection.geojson || null,
      };
      zone =
        requestedMode === "area"
          ? {
              ...deriveRadiusZoneFromArea(baseZone),
              label: baseZone.label,
              source: baseZone.source,
              raw_query: baseZone.raw_query,
              admin_level: baseZone.admin_level,
            }
          : {
              ...baseZone,
              mode: "radius_from_point",
              radius_bucket: baseZone.radius_bucket || "10km",
              radius_km: normalizeRadius(baseZone.radius_bucket || "10km").radiusKm,
              bbox: null,
              geojson: null,
            };
      preferredMode = "radius";
      adminZoneCache = null;
      manualZoneCache = JSON.parse(JSON.stringify(zone));
      drawZone();
    };

    const fetchGeocodeItems = async (query, preferredCountry = "") => {
      const params = new URLSearchParams({ q: query });
      if (preferredCountry) {
        params.set("countrycodes", preferredCountry);
      }
      const response = await fetch(`/api/geocode/search?${params.toString()}`, {
        headers: { Accept: "application/json" },
      });
      if (!response.ok) return [];
      const data = await response.json();
      return data.items || [];
    };

    const pickBestItem = (query, items) => {
      if (!items.length) return null;
      const needle = query.trim().toLowerCase();
      const scored = [...items].sort((left, right) => scoreItem(right) - scoreItem(left));
      return scored[0];

      function scoreItem(item) {
        const label = String(item.full_label || item.label || "").toLowerCase();
        const firstPart = label.split(",")[0].trim();
        const normalizedFirst = normalizeGeoName(firstPart);
        const normalizedNeedle = normalizeGeoName(query);
        let score = 0;
        if (isSpanishAdminQuery(query) && normalizedFirst === normalizedNeedle) score += 140;
        if (item.mode === "area" && item.geojson) score += 90;
        if (item.mode === "area") score += 40;
        if (firstPart === needle) score += 100;
        if (firstPart.startsWith(needle)) score += 40;
        if (label.includes(", españa") || label.includes(", spain")) score += 20;
        if (label.includes(", catalunya") || label.includes(", cataluña")) score += 10;
        return score;
      }
    };

    const fetchSuggestions = debounce(async () => {
      const query = searchInput.value.trim();
      if (query.length < 2) {
        renderResults([]);
        return;
      }
      try {
        let items = [];
        if (countryBias) {
          items = await fetchGeocodeItems(query, countryBias);
        }
        if (!items.length) {
          items = await fetchGeocodeItems(query);
        }
        renderResults(items);
      } catch (_) {}
    }, 300);

    const autoResolveInitialLabel = async () => {
      const query = (zone.label || searchInput?.value || "").trim();
      const hasResolvedRadius =
        zone.mode !== "area" && zone.center && zone.center.lat != null && zone.center.lon != null;
      const hasResolvedArea =
        zone.mode === "area" && zone.geojson && typeof zone.geojson === "object";
      if (!query || hasResolvedRadius || hasResolvedArea) {
        return;
      }
      try {
        let items = [];
        if (countryBias) {
          items = await fetchGeocodeItems(query, countryBias);
        }
        if (!items.length) {
          items = await fetchGeocodeItems(query);
        }
        const best = pickBestItem(query, items);
        if (!best) return;
        applySelection({
          forceMode: best.mode || "radius_from_point",
          mode: best.mode || "radius_from_point",
          label: best.label,
          center: { lat: best.lat, lon: best.lon },
          source: best.source || "autocomplete",
          raw_query: query,
          admin_level: best.admin_level || "",
          bbox: best.bbox || null,
          geojson: best.geojson || null,
        });
      } catch (_) {}
    };

    searchInput?.addEventListener("input", fetchSuggestions);
    searchInput?.addEventListener("input", () => {
      if (clearButton) clearButton.hidden = !searchInput.value.trim();
    });
    searchInput?.addEventListener("focus", () => {
      if (suppressNextFocusOpen) {
        suppressNextFocusOpen = false;
        return;
      }
      if (resultsBox.children.length) resultsBox.hidden = false;
    });

    clearButton?.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      if (modalMode) {
        zone = defaultZone();
        preferredMode = "radius";
        adminZoneCache = null;
        manualZoneCache = null;
        searchInput.value = "";
        resultsBox.innerHTML = "";
        hideResults();
        clearButton.hidden = true;
        drawZone();
        fitDefaultView();
        suppressNextFocusOpen = true;
        searchInput.blur();
        return;
      }
      clearZoneState(true);
    });

    document.addEventListener("click", (event) => {
      if (!root.contains(event.target)) {
        hideResults();
      }
    });

    mapContainer.addEventListener("pointerdown", () => {
      hideResults();
      suppressNextFocusOpen = true;
      searchInput?.blur();
    });

    radiusInputs.forEach((input) => {
      input.addEventListener("change", () => {
        zone.mode = "radius_from_point";
        preferredMode = "radius";
        zone.radius_bucket = input.value;
        zone.radius_km = normalizeRadius(input.value).radiusKm;
        zone.geojson = null;
        zone.bbox = null;
        zone.admin_level = "";
        manualZoneCache = JSON.parse(JSON.stringify(zone));
        drawZone();
      });
    });

    modeButtons.forEach((button) => {
      button.addEventListener("click", () => {
        const targetMode = button.dataset.zoneMode;
        if (targetMode === "area") {
          preferredMode = "area";
          if (zone.mode === "radius_from_point" && (zone.center?.lat != null || zone.label)) {
            manualZoneCache = JSON.parse(JSON.stringify(zone));
          }
          if (adminZoneCache) {
            zone = JSON.parse(JSON.stringify(adminZoneCache));
          }
          if (collapsibleMap) {
            mapPanel?.classList.remove("is-collapsed");
          }
          drawZone();
          window.setTimeout(() => {
            map.invalidateSize();
            drawZone();
          }, 80);
          return;
        }

        preferredMode = "radius";
        if (zone.mode === "area") {
          adminZoneCache = JSON.parse(JSON.stringify(zone));
        }
        const previousMapCenter = map.getCenter();
        const previousMapZoom = map.getZoom();
        if (zone.mode === "area") {
          zone = deriveRadiusZoneFromArea(zone);
          zone.label = "Área personalizada";
          zone.raw_query = "Área personalizada";
          manualZoneCache = JSON.parse(JSON.stringify(zone));
        } else {
          zone = manualZoneCache ? JSON.parse(JSON.stringify(manualZoneCache)) : defaultZone();
          zone.mode = "radius_from_point";
          zone.geojson = null;
          zone.bbox = null;
          zone.admin_level = "";
          zone.label = "Área personalizada";
          zone.raw_query = "Área personalizada";
        }
        if (searchInput) {
          searchInput.value = "Área personalizada";
        }
        hiddenLabel.value = "Área personalizada";
        if (collapsibleMap) {
          mapPanel?.classList.remove("is-collapsed");
        }
        drawZone();
        window.setTimeout(() => {
          map.invalidateSize();
          if (previousMapCenter && Number.isFinite(previousMapZoom)) {
            map.setView(previousMapCenter, previousMapZoom);
          }
        }, 80);
      });
    });

    toggleMapButton?.addEventListener("click", () => {
      mapPanel?.classList.toggle("is-collapsed");
      setTimeout(() => map.invalidateSize(), 40);
    });

    summaryTriggers.forEach((trigger) => {
      trigger.addEventListener("click", (event) => {
        event.preventDefault();
        openModal();
      });
    });

    summaryClearButton?.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      clearZoneState(true);
    });

    modalCloseButtons.forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        closeModal(true);
      });
    });

    modalClearButton?.addEventListener("click", (event) => {
      event.preventDefault();
      zone = defaultZone();
      preferredMode = "radius";
      adminZoneCache = null;
      manualZoneCache = null;
      if (searchInput) searchInput.value = "";
      resultsBox.innerHTML = "";
      hideResults();
      drawZone();
      fitDefaultView();
    });

    modalApplyButton?.addEventListener("click", (event) => {
      event.preventDefault();
      appliedZone = cloneZone(zone);
      syncHiddenFields();
      closeModal(false);
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && isModalOpen()) {
        closeModal(true);
      }
    });

    map.on("click", async (event) => {
      if (preferredMode !== "radius") {
        hideResults();
        return;
      }
      hideResults();
      const lat = Number(event.latlng.lat.toFixed(6));
      const lon = Number(event.latlng.lng.toFixed(6));
      try {
        const response = await fetch(`/api/geocode/reverse?lat=${lat}&lon=${lon}`, {
          headers: { Accept: "application/json" },
        });
        const data = response.ok ? await response.json() : {};
        applySelection({
          forceMode: "radius_from_point",
          mode: "radius_from_point",
          label: data.label || "Punto seleccionado en el mapa",
          center: { lat, lon },
          source: "map_click",
          raw_query: searchInput.value.trim(),
          admin_level: "",
          bbox: null,
          geojson: null,
        });
      } catch (_) {
        applySelection({
          forceMode: "radius_from_point",
          mode: "radius_from_point",
          label: "Punto seleccionado en el mapa",
          center: { lat, lon },
          source: "map_click",
          raw_query: searchInput.value.trim(),
          admin_level: "",
          bbox: null,
          geojson: null,
        });
      }
    });

    if (zone.mode === "area" || (zone.center && zone.center.lat != null && zone.center.lon != null)) {
      drawZone();
    } else {
      syncHiddenFields();
      fitDefaultView();
      autoResolveInitialLabel();
    }
    setTimeout(() => map.invalidateSize(), 40);
  };

  window.addEventListener("load", () => {
    document.querySelectorAll('[data-zone-selector="true"]').forEach(initSelector);
  });
})();
