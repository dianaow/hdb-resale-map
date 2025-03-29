import * as d3 from 'd3';
import { Map, Popup, LngLatBounds } from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';

var accessToken = import.meta.env.VITE_ACCESS_TOKEN
var style = import.meta.env.VITE_MAPBOX_STYLE

var map, GLOBAL_DATA, GLOBAL_AGG_PRICES_DATA, GLOBAL_PRICES_DATA, GLOBAL_STREETS_PRICES
var selectedTowns = []
var selectedStreets = []
var selectedFlatType = '4 ROOM'
var selectedLegendStatus = 'type'
var chartType = 'town'
var year_range = [1960, new Date().getFullYear()]
var years = d3.range(year_range[0], year_range[1])
var newRange = [new Date(year_range[0], 0, 1), new Date(year_range[1], 11, 31)]

var parseYear = d3.timeParse("%Y") // creates a function that parses a string representing a year into a JavaScript Date object

var colorMap = {
  "Miscellaneous": "#22d3ee",
  "Residential": "#FF00FF",
  "Market and hawker": "#FFD700",
  "Multi-storey carpark": "#d6d3d1",  // New color for fish
  "Commercial": "#FF7F50"
};
var tags = Object.keys(colorMap)
var color = d3.scaleOrdinal()
  .domain(tags)
  .range(Object.values(colorMap))

var colorAge = d3.scaleSequential()
  .domain([0, 50])
  .interpolator(d3.interpolateBuPu)

var thresholds = [0, 300000, 600000, 800000, 1000000];
var colors = ["white", "#99f6e4", "#2dd4bf", "#FF7F50", "#FFD700"];
var colorPrice = d3.scaleThreshold()
  .domain(thresholds.slice(1))  // Skip the first threshold (0) as domain needs one fewer entry than range
  .range(colors);
  
var axisColor = "#d4d4d4"

async function init() {
  const endpoints = [
    '/api/properties',
    '/api/agg_prices',
    '/api/geojson',
    '/api/agg_address_prices'
  ];
  
  const fetchPromises = endpoints.map(endpoint => 
    fetch(endpoint)
      .then(response => {
        if (!response.ok) {
          throw new Error(`HTTP error ${response.status} for ${endpoint}`);
        }
        return response.json();
      })
  );

  const results = await Promise.all(fetchPromises);

  initVisuals(results[0].properties, JSON.parse(results[1].prices), results[2].geojson, JSON.parse(results[3].prices))
}

async function initVisuals(data, prices, geojson, streetPrices) {
  data.forEach((d, i) => {
    d.age = new Date().getFullYear() - (+d['year'])
    d.date = parseYear(d['year'])
    d.block = extractFirstNumber(d['address'])
    d.color =  color(d.tag),
    d.ageColor = colorAge(d.age)
  })
  data.forEach((d, i) => {
    const point = streetPrices
      .filter(el => el.flat_type === selectedFlatType)
      .find(el => el['block_street'] === (d['block'] + ' ' + d['street']))
    if(point) {
      d.price = isValidNumber(point.price) ? point.price : "NA"
      d.priceColor = isValidNumber(point.price) ? colorPrice(point.price) : 'gray'
    } else {
      d.price = "NA"
      d.priceColor = 'gray'
    }
  })

  data = data.filter(d => d.date > new Date(year_range[0], 1, 1))
  data = data.sort(function (a, b) { return d3.ascending(a.date, b.date) })
  GLOBAL_DATA = [...data]
  GLOBAL_STREETS_PRICES = [...streetPrices]

  await initMap(data)
  
  createTownsDropdown(geojson)

  d3.select('#properties-total').html(data.length)
  d3.select('#years').html(year_range[0] + " and " + year_range[1])

  multipleLineChartBrush(data)

  prices.forEach(d => {
    d.date = parseQuarter(d.quarter)
    d.price = isValidNumber(d.price) ? +d.price : null
  })
  GLOBAL_AGG_PRICES_DATA = [...prices]
  
  createFlatTypeDropdown()

  const filteredData = GLOBAL_AGG_PRICES_DATA.filter(d => d.flat_type === selectedFlatType)

  multipleLineChart(filteredData, { groupBy: 'town' })

  createColorDropdown()
}

///////////////////////////////////////////////////////////////////////////
/////////////////////////////// Data processing ///////////////////////////
///////////////////////////////////////////////////////////////////////////
function nestData(data) {
  var dataByCountry = Array.from(
    d3.rollup(
      data,
      leaves => leaves.length,
      d => d.date,      
      d => d.tag              
    )
  )
    .filter(([key, value]) => value != 0)
    .map(([key, value]) => ({
      key,
      values: Array.from(value).sort((a, b) => tags.indexOf(a[0]) - tags.indexOf(b[0])) // sort tags
    }))
    .map(d => ({
      ...d,
      values: d.values.map(inner => ({
        key: inner[0],
        value: inner[1]
      }))
    }));

  var json = []
  dataByCountry.map((a, idx) => {
    //if(idx<=20){
    tags.map((b, i) => {
      if (a.values[i]) {
        json.push({
          'town': a.key,
          'tag': a.values[i].key,
          'value': a.values[i].value,
        })
      }
    })
    //}
  })

  let dataNew = []
  tags.map(s => {
    var cumsum_arr = []
    years.map(year => {
      var label = parseYear(year)
      var tmp = json.find(d => d.tag == s && d.town.getTime() === label.getTime())
      if (tmp) { cumsum_arr.push(tmp.value) }
      var cumsum = cumsum_arr.reduce(function (a, b, i) { return a + b; }, 0);
      dataNew.push({
        tag: s,
        town: label,
        value: cumsum
      })
    })
  })

  return dataNew
}

///////////////////////////////////////////////////////////////////////////
/////////////////////////////////// Map  //////////////////////////////////
///////////////////////////////////////////////////////////////////////////
function initMap(data) {
  return new Promise((resolve) => {
    map = new Map({
      container: 'map',
      style,
      accessToken,
      center: [103.9, 1.35],
      zoom: 12.2,
      antialias: true,
      maxZoom: 18,
      minZoom: 10
    });
    
    var dotsGeoJSON = { "type": "FeatureCollection", "features": [] }
    data.map((d, i) => {
      dotsGeoJSON.features.push({
        type: "Feature",
        geometry: {
          type: "Point",
          coordinates: [d.lon, d.lat]
        },
        properties: {
          ...d
        }
      })
    })
  
    map.on('load', function () {
      if (dotsGeoJSON.features.length > 0) {
        map.addSource('markers', {
          type: 'geojson',
          data: dotsGeoJSON,
          cluster: false,
          clusterMaxZoom: 13, // Max zoom to cluster points on
        });
  
        map.addLayer({
          id: 'clusters',
          type: 'circle',
          source: 'markers',
          filter: ['has', 'point_count'],
          paint: {
              'circle-color': "#FF00FF",
              'circle-radius': [
                  'step',
                  ['get', 'point_count'],
                  8, 50,   // Small clusters (< 50 points) have a radius of 10
                  16, 100,  // Clusters with 50-100 points have a radius of 16
                  24, 300,  // Clusters with 100-300 points have a radius of 24
                  36, 800,  // Clusters with 300-800 points have a radius of 36
                  48        // Clusters with more than 800 points have a radius of 48
              ]
          }
        });
  
        map.addLayer({
          id: 'cluster-count',
          type: 'symbol',
          source: 'markers',
          filter: ['has', 'point_count'],
          layout: {
              'text-field': ['get', 'point_count_abbreviated'],
              'text-font': ['DIN Offc Pro Medium', 'Arial Unicode MS Bold'],
              'text-size': 12
          }
        });
  
        map.addLayer({
          'id': 'circle',
          'type': 'circle',
          'source': 'markers',
          filter: ['!', ['has', 'point_count']],
          'paint': {
            // make circles larger as the user zooms in
            'circle-radius': [
              'interpolate', 
              ['linear'], 
              ['zoom'], 
              12, [
                'interpolate', 
                ['linear'], 
                ['get', 'total_units'],
                0, 1.5,    // For total_units 0, set radius to 1
                50, 1.5,  
                100, 2,
                200, 3, 
                300, 5
              ],
              18, [
                'interpolate', 
                ['linear'], 
                ['get', 'total_units'],
                0, 3, // For total_units 0, set radius to 1
                50, 5,
                100, 7,
                200, 11,
                300, 15
              ]
            ],
            'circle-color': ['get', 'color']
          }
        })
  
        tags.forEach(tag => {
          addSvgMarkerToMap(map, tag)
        })
  
        createMapMisc()
      }
  
      // Create a single popup for all markers, but don't add it to the map yet.
      const popup = new Popup({
        closeButton: false,
        closeOnClick: false,
        anchor: 'top'
      });
  
      activateMarkerActions(map, popup, 'circle')
      tags.forEach(tag => {
        activateMarkerActions(map, popup, tag + '-icon')
      })

      resolve();
    })
  })
}

function activateMarkerActions(map, popup, layerID) {
  map.on('mouseenter', layerID, function (e) {
    // Change the cursor style as a UI indicator.
    map.getCanvas().style.cursor = 'pointer';
    let coordinates = e.features[0].geometry.coordinates.slice();
    let properties = e.features[0].properties

    let description = `
    <div>
      <p style="font-weight: bold; font-size: 13px; color: black">${properties.address}</p>
      <h4>${properties.street}</h4>
      <h4>Completed in: ${new Date(properties.date).getFullYear()}</h4>
      <h4>Total units: ${properties.total_units}</h4>
      <h4>Max floor level: ${properties.max_floor_lvl}</h4>
    </div>
  `;

    // Ensure that if the map is zoomed out such that multiple
    // copies of the feature are visible, the popup appears
    // over the copy being pointed to.
    while (Math.abs(e.lngLat.lng - coordinates[0]) > 180) {
      coordinates[0] += e.lngLat.lng > coordinates[0] ? 360 : -360;
    }

    // Populate the popup and set its coordinates
    // based on the feature found.
    popup
      .setLngLat(coordinates)
      .setHTML(description)
      .addTo(map);
  });

  map.on('mouseleave', layerID, function (e) {
    map.getCanvas().style.cursor = '';
    popup.remove();
  });

  map.on('click', layerID, function (e) {
    if (selectedTowns.length === 0 || selectedTowns[0] === 'All Towns' || chartType === 'town') return // Only allow click on marker if a town is selected
  
    const street = e.features[0].properties.street
    selectedStreets.push(street)

    window.highlightDots({groupBy: 'street', justClickedName: street});

    const filteredPoints = GLOBAL_DATA.filter(point => point.street === street);
    addHighlightLayer(filteredPoints, street + '-highlight', 'black');
  })
}

function addSvgMarkerToMap(map, markerId, color) {
  // Create the SVG with the specified circle color but with smaller initial dimensions
  const circleSVG = `
    <svg xmlns="http://www.w3.org/2000/svg" width="40" height="40" viewBox="0 0 40 40">
      <circle cx="20" cy="20" r="16" fill="${colorMap[markerId] || color}" />
    </svg>`;

  const svgUrl = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(circleSVG)}`;

  // Create an image element with smaller initial size
  const img = new Image(20, 20); // Reduced from 40,40 for better initial scale
  
  img.onload = () => {
    map.addImage(markerId, img);

    map.addLayer({
      id: `${markerId}-icon`,
      type: 'symbol',
      source: 'markers',
      filter: [
        'all',
        ['!', ['has', 'point_count']],
        ['==', ['get', 'tag'], markerId]
      ],
      layout: {
        visibility: 'none',
        'icon-image': markerId,
        'icon-size': [
          'interpolate',
          ['linear'],
          ['zoom'],
          // Start scaling earlier and use more intermediate zoom levels
          10, [ // Start at zoom 10 instead of 12
            'interpolate',
            ['linear'],
            ['get', 'total_units'],
            0, 0.3,    // Smaller initial size
            50, 0.4,
            100, 0.5,
            200, 0.6,
            300, 0.7
          ],
          12, [ // Add intermediate zoom level
            'interpolate',
            ['linear'],
            ['get', 'total_units'],
            0, 0.5,
            50, 0.6,
            100, 0.7,
            200, 0.8,
            300, 0.9
          ],
          15, [ // Add intermediate zoom level
            'interpolate',
            ['linear'],
            ['get', 'total_units'],
            0, 0.7,
            50, 0.8,
            100, 0.9,
            200, 1.0,
            300, 1.1
          ],
          18, [
            'interpolate',
            ['linear'],
            ['get', 'total_units'],
            0, 0.9,
            50, 1.0,
            100, 1.1,
            200, 1.2,
            300, 1.3
          ]
        ],
        'icon-pitch-alignment': 'map',
        'icon-rotation-alignment': 'map',
        'symbol-spacing': 1,
        'icon-allow-overlap': true,
        'text-allow-overlap': true
      },
    });
  };

  img.src = svgUrl;
}

function toggleClustering(map) {
  // Get the current state - check if the source exists and if clustering is enabled
  let isCurrentlyClustered = false;
  const source = map.getSource('markers');
  
  if (source) {
    // Get the current source data
    const currentData = map.getSource('markers')._data;
    
    // Check if clustering is currently enabled
    isCurrentlyClustered = source._options.cluster === true;
    
    // Store which layers use this source before removing
    const dependentLayers = [];
    map.getStyle().layers.forEach(layer => {
      if (layer.source === 'markers') {
        dependentLayers.push(layer);
      }
    });
    
    // Remove all layers that use this source
    dependentLayers.forEach(layer => {
      map.removeLayer(layer.id);
    });
    
    // Remove the source
    map.removeSource('markers');
    
    // Add the source back with toggled clustering
    map.addSource('markers', {
      type: 'geojson',
      data: currentData,
      cluster: !isCurrentlyClustered,
      clusterMaxZoom: 13,
      clusterRadius: 50
    });
    
    // Re-add all the layers that were using this source
    dependentLayers.forEach(layer => {
      // Create a new layer definition with the same properties
      const newLayer = {...layer};
      map.addLayer(newLayer);
    });
    
    // Return the new clustering state
    return !isCurrentlyClustered;
  }
  
  return false;
}

function updateMarkersMap(data) {
  const dotsGeoJSON = { "type": "FeatureCollection", "features": [] }

  data.map((d, i) => {
    dotsGeoJSON.features.push({
      type: "Feature",
      geometry: {
        type: "Point",
        coordinates: [d.lon, d.lat]
      },
      properties: {
        ...d,
        color: color(d.tag),
        ageColor: colorAge(d.age)
      }
    })
  })

  map.getSource('markers').setData(dotsGeoJSON)
}

function removeHighlightLayer(layerId) {
  // Check if the layer and source exist before attempting to remove them
  if (map.getLayer(layerId)) {
    map.removeLayer(layerId); // Remove the layer first
  }

  if (map.getSource(layerId)) {
    map.removeSource(layerId); // Then remove the source
  }
}

function addHighlightLayer(points, layerId, color) {
  let totalLat = 0;
  let totalLon = 0;
  let dotsGeoJSON = { "type": "FeatureCollection", "features": [] };
  points.forEach(point => {
    totalLat += point.lat;
    totalLon += point.lon;
    dotsGeoJSON.features.push({
      type: "Feature",
      geometry: {
        type: "Point",
        coordinates: [point.lon, point.lat]  // Use coordinates from the data point
      },
      properties: {
        ...point
      }
    });        
  });
  
  const centroid = {
    lat: totalLat / points.length,
    lon: totalLon / points.length
  };
  
  if (centroid.lat && centroid.lon) {
    map.flyTo({
      center: [centroid.lon, centroid.lat],
      zoom: 15
    });
    
    // Add source if it doesn't exist
    if (!map.getSource(layerId)) {
      map.addSource(layerId, {
        type: 'geojson',
        data: dotsGeoJSON
      });
    } else {
      // Update the source data
      map.getSource(layerId).setData(dotsGeoJSON);
    }
    
    map.addLayer({
      'id': layerId,
      'type': 'circle',
      'source': layerId,
      'paint': {
        // make circles larger as the user zooms in
        'circle-radius': [
          'interpolate', 
          ['linear'], 
          ['zoom'], 
          12, [
            'interpolate', 
            ['linear'], 
            ['get', 'total_units'],
              0, 1.5,    // For total_units 0, set radius to 1
              50, 1.5,  
              100, 2,
              200, 3, 
              300, 5
          ],
          18, [
            'interpolate', 
            ['linear'], 
            ['get', 'total_units'],
            0, 3, // For total_units 0, set radius to 1
            50, 5,
            100, 7,
            200, 11,
            300, 15
          ]
        ],
        'circle-color': 'rgba(0, 0, 0, 0)',  // Transparent fill
        'circle-opacity': 1,
        'circle-stroke-width': 1.5,          // Border width
        'circle-stroke-color': color       // Border color
      }
    });
  }
}

function syncHighlightLayers(prevSelection, currentSelection) {
  // Find values that have been removed
  const removedValues = prevSelection.filter(value => 
    !currentSelection.includes(value) && value !== 'All Towns'
  );
  
  // Find values that have been added
  const addedValues = currentSelection.filter(value => 
    !prevSelection.includes(value) && value !== 'All Towns'
  );
  
  // Remove layers for removed values
  removedValues.forEach(value => {
    const layerId = value + '-highlight';
    removeHighlightLayer(layerId);
  });
  
  // Add layers for added values
  addedValues.forEach(value => {
    const filteredPoints = GLOBAL_DATA.filter(point => point.town === value);
    addHighlightLayer(filteredPoints, value + '-highlight', '#fff');
  });
}

function zoomToCentroid(geojson) {
  // Create an initial bounds object that we'll expand to include all towns
  let combinedBounds = null;

  // Loop through all selected towns
  for (const townName of selectedTowns) {
    // Find the feature for this town
    const townFeature = geojson.features.find(d => d.properties.PLN_AREA_N === townName);
    
    if (townFeature) {
      const coordinates = townFeature.geometry.coordinates;
      
      // For each town's coordinates, create or extend the bounds
      if (!combinedBounds) {
        // Initialize bounds with the first town's first coordinate pair
        combinedBounds = new LngLatBounds(coordinates[0][0], coordinates[0][0]);
      }
      
      // Extend the bounds to include all coordinates of this town
      coordinates[0].forEach(coord => {
        combinedBounds.extend(coord);
      });
    }
  }

  // If we have valid bounds, fit the map to them
  if (combinedBounds) {
    map.fitBounds(combinedBounds, {
      padding: 20, // Add some padding around the bounds
      zoom: selectedTowns.length > 1 ? 12.5 : 14
    });
  } else {
    // Fallback if no valid bounds were found
    map.flyTo({
      center: [103.9, 1.35],
      zoom: 12.2
    });
        }
}
///////////////////////////////////////////////////////////////////////////
//////////////////////////// Multiple line chart //////////////////////////
//////////////////////////////////////////art/////////////////////////////////
function multipleLineChartBrush(data) {
  d3.selectAll(".tooltip-group").style('opacity', 0)

  const dataNew = nestData(data)
  
  const res_nested = Array.from(d3.group(dataNew, d => d.tag))
    .map(([key, values]) => ({
      key,
      values: values.sort((a, b) => years.indexOf(parseYear(a.town)) - years.indexOf(parseYear(b.town))) // Sort values based on parsed years
    }));

  const chart = d3.select("#chart-timeline")
  chart.selectAll("*").remove();

  const rect = chart.node().getBoundingClientRect();
  const svg = chart.append("svg")
    .attr("width", rect.width)
    .attr("height", rect.height)

  const group = svg.append('g')

  const margin = { top: 20, right: 30, bottom: 30, left: 30 }

  const xScale = d3.scaleTime()
  .domain(d3.extent(years, function (d) { return parseYear(d) }))
  .range([margin.left, rect.width - margin.right])

  const yScale = d3.scaleSqrt()
    .domain([0, d3.max(res_nested.map(d => d.values).flat(), d => d.value)])
    .range([rect.height - margin.bottom, margin.top]);

  // Create a chart title
  group.append('text')
    .attr('class', 'title')
    .attr("transform", `translate(${5},${10})`)
    //.style('text-anchor', 'middle')
    .style('font-size', '12px')
    .style('font-family', 'Montserrat')
    .attr('fill', '#fff')
    .text('Cumulative number of HDB properties completed');

  group.append('text')
    .attr('class', 'subtitle')
    .attr("transform", `translate(${(rect.width - margin.right) / 2},${26})`)
    .style('text-anchor', 'middle')
    .style('font-size', '11px')
    .attr('fill', "#9ca3af")
    .text('Drag to select time range');

  // Add x-axis
  group.append("g")
    .attr("transform", `translate(0,${rect.height - margin.bottom})`)
    .call(d3.axisBottom(xScale).tickSize(0).ticks(6))
    .call(g => {
      g.selectAll("text")
        .attr('fill', axisColor)
        .style('font-size', '11px');
      g.selectAll("line")
        .attr('stroke', axisColor);
      g.select(".domain").remove();
    });

  // Add Y-axis
  group.append("g")
    .attr("transform", `translate(${margin.left},0)`)
    .call(d3.axisLeft(yScale).tickSize(-rect.width + margin.right).ticks(3).tickFormat(d3.format("~s")))
    .call(g => {
      g.selectAll("line")
        .attr('stroke', '#525252')
        .attr('stroke-width', 0.7) // make horizontal tick thinner and lighter so that line paths can stand out
        .attr('opacity', 0.3)

      g.selectAll("text")
        .attr('fill', axisColor)
        .style('font-size', '10px');
      g.select(".domain").remove();
    });

  const line = d3.line()
    .x(function (d) { return xScale(d.town) })
    .y(function (d) { return yScale(d.value) })

  const glines = group.selectAll('.line-group').data(res_nested, d => d.key)

  const entered_lines = glines.enter().append('g').attr('class', 'line-group')

  entered_lines.append('path').attr('class', 'line')

  glines.merge(entered_lines).select('.line')
    .attr('d', function (d) { return line(d.values) })
    .style('stroke', (d, i) => color(d.key))
    .style('fill', 'none')
    .style('opacity', 0.8)
    .style('stroke-width', '1.5px')
    .style('stroke-cap', 'round')

  glines.exit().remove()

  // BRUSH
  const brush = d3.brushX().extent([[margin.left, margin.top], [rect.width - margin.right, rect.height - margin.bottom]])

  group.append("g")
    .attr("class", "brush")
    .call(brush)

  brush.on("brush end", brushed)

  function brushed(event) {
    if (event.sourceEvent && event.sourceEvent.type === "zoom") return; // ignore brush-by-zoom

    // Get the selection from the event object
    var s = event.selection || xScale.range();

    // Map the selected range to the xScale domain (i.e., convert pixel values to data values)
    newRange = s.map(xScale.invert, xScale);

    const dataNew = data.filter(d => (d.date >= newRange[0]) & (d.date <= newRange[1]))

    if(selectedTowns.length === 0 || selectedTowns[0] === 'All Towns') {
      // Update elements with the full dataset and new range
      updateMarkersMap(dataNew);
      d3.select('#properties-total').html(dataNew.length)
    } else {
      // Filter data based on the selected town(s)
      const selectedData = dataNew.filter(d => selectedTowns.indexOf(d.town) !== -1)
      updateMarkersMap(selectedData);
      d3.select('#properties-total').html(selectedData.length)
    }

    const ranges = newRange.map(d => d.getFullYear().toString())
    d3.select('#years').html(ranges[0] + " and " + ranges[1])
  }
}

function multipleLineChart(data, userOptions = {}) {
  d3.selectAll(".tooltip-group").remove()

  const defaultOptions = { 
    container: '#chart-resale-prices',
    y: 'price', 
    groupBy: 'town', 
    title: null, 
    subtitle: 'Click on a circle to locate on map. Click again to deselect.'
  };
  // Merge default options with user-provided options
  const options = { ...defaultOptions, ...userOptions };
  const { container, groupBy, title, subtitle, y } = options;

  chartType = groupBy

  const margin = { top: 10, right: 20, bottom: 30, left: 30 };
  const transitionDuration = 750; // Duration for transitions in milliseconds

  // Select the chart container
  const chart = d3.select(container);
  const rect = chart.node().getBoundingClientRect();
  
  // Ensure SVG exists
  let svg = chart.selectAll("svg").data([0]);
  svg = svg.join(
    enter => enter.append("svg")
      .attr("width", rect.width)
      .attr("height", rect.height),
    update => update
      .attr("width", rect.width)
      .attr("height", rect.height)
  );
  
  // Ensure main group exists
  let group = svg.selectAll("g.main-group").data([0]);
  group = group.join(
    enter => enter.append("g").attr("class", "main-group")
  );

  // Check if data is empty
  if (!data || data.length === 0) {
    // Remove existing no-data text if present
    group.selectAll(".no-data-text").remove();
    
    // Add "No Resale Data" text
    group.append("text")
      .attr("class", "no-data-text")
      .attr("x", rect.width / 2)
      .attr("y", rect.height / 2)
      .attr("text-anchor", "middle")
      .attr("dominant-baseline", "middle")
      .style("font-size", "16px")
      .style("font-weight", "bold")
      .text("No Resale Data");
      
    return; // Exit the function early
  } else {
    // Remove no-data text if it exists
    group.selectAll(".no-data-text").remove();
  }
  
  // Get the current date and calculate the date 6 months ago
  const sixMonthsAgo = d3.extent(data, d => d.date)[1];
  sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 5);
  
  // Group data by the `groupBy` field (e.g., 'town', 'flat_type')
  const res_nested = Array.from(d3.group(data, d => d[groupBy]))
    .map(([key, values]) => ({
      key,
      values: values.sort((a, b) => new Date(a.date) - new Date(b.date))
    }));

  // Calculate total average price for the last year, used for highlighting and sorting
  const res_nested2 = Array.from(d3.group(data, d => d[groupBy]))
    .map(([key, values]) => {
      const totalAveragePrice = d3.mean(
        values.filter(d => new Date(d.date) >= sixMonthsAgo),
        d => d[y]
      );
      return {
        key,
        values: values.sort((a, b) => new Date(a.date) - new Date(b.date)),
        totalAveragePrice,
      };
    })
    .filter(d => d.totalAveragePrice)
    .sort((a, b) => b.totalAveragePrice - a.totalAveragePrice); // Sort by total average price

  // Highlight top 3 and bottom 3 streets
  const topThree = res_nested2.slice(0, 3);
  const bottomThree = res_nested2.slice(-2).reverse();
  const highlighted = new Set([...topThree, ...bottomThree].map(d => d.key));

  const colorScale = d3.scaleOrdinal()
    .domain(highlighted)
    .range(['#a21caf', '#d946ef', '#f0abfc', '#f0abfc', '#d946ef', '#a21caf']);

  const xScale = d3.scaleTime()
    // .domain([
    //   d3.timeYear.round(d3.extent(data, d => d.date)[0]),  // Round to nearest year for min
    //   d3.timeYear.round(d3.extent(data, d => d.date)[1])   // Round to nearest year for max
    // ])
    .domain([
      d3.timeYear.round(d3.extent(data, d => d.date)[0]),
      d3.extent(data, d => d.date)[1]   
    ])
    .range([margin.left, rect.width - margin.right]);

  // Update or create x-axis using join
  let xAxis = group.selectAll(".x-axis").data([0]);
  xAxis = xAxis.join(
    enter => enter.append("g")
      .attr("class", "x-axis")
      .attr("transform", `translate(0,${rect.height - margin.bottom})`)
  );
  
  xAxis
    .transition()
    .duration(transitionDuration)
    .attr("transform", `translate(0,${rect.height - margin.bottom})`)
    .call(d3.axisBottom(xScale).tickSize(0).ticks(6))
    .call(g => {
      g.selectAll("text")
        .attr('fill', axisColor)
        .style('font-size', '11px');
      g.selectAll("line")
        .attr('stroke', axisColor);
      g.select(".domain").remove();
    });

  // Update or create title using join
  let titleElement = group.selectAll(".title").data(title ? [title] : []);
  titleElement = titleElement.join(
    enter => enter.append("text")
      .attr("class", "title")
      .attr("transform", `translate(${(rect.width - margin.right) / 2},${margin.top})`)
      .style("text-anchor", "middle")
      .style("font-size", "15px")
      .attr("fill", axisColor)
      .text(d => d),
    update => update
      .transition()
      .duration(transitionDuration)
      .attr("transform", `translate(${(rect.width - margin.right) / 2},${margin.top})`)
      .text(d => d)
  );

  // Update or create subtitle using join
  let subtitleElement = group.selectAll(".subtitle").data(subtitle ? [subtitle] : []);
  subtitleElement = subtitleElement.join(
    enter => enter.append("text")
      .attr("class", "subtitle")
      .attr("transform", `translate(${(rect.width - margin.right) / 2},${margin.top})`)
      .style("text-anchor", "middle")
      .style("font-size", "11px")
      .attr("fill", "#9ca3af")
      .text(d => d),
    update => update
      .transition()
      .duration(transitionDuration)
      .attr("transform", `translate(${(rect.width - margin.right) / 2},${margin.top})`)
      .text(d => d)
  );

  // Update the legend group in the top-left corner
  let legend = group.selectAll(".chart-legend").data([0]);
  legend = legend.join(
    enter => enter.append("g")
      .attr("class", "chart-legend")
      .attr("transform", `translate(${margin.left + 5}, ${margin.top + 16})`)
  );

  const fromDate = sixMonthsAgo.toLocaleDateString(undefined, {
    year: 'numeric',
    month: 'short',
  });
  const toDate = d3.extent(data, d => d.date)[1].toLocaleDateString(undefined, {
    year: 'numeric',
    month: 'short',
  });
  
  // Add title for highest group
  let highestTitle = legend.selectAll(".highest-title").data([0]);
  highestTitle = highestTitle.join(
    enter => enter.append("text")
      .attr("class", "highest-title")
      .attr("y", 0)
      .style("font-size", "11px")
      .style("font-weight", "bold")
      .attr("fill", axisColor)
      .text(`Highest resale price from ${fromDate} to  ${toDate}`),
    update => update
    .text(`Highest resale price from ${fromDate} to  ${toDate}`)
  );

  // Add top three items
  const topThreeItems = legend.selectAll(".top-item")
    .data(topThree);

  topThreeItems.join(
    enter => enter.append("text")
      .attr("class", d => `top-item item-${makeSafeKey(d.key)}`)
      .attr("y", (d, i) => 16 + i * 16)
      .style("font-size", "10px")
      .style("fill", d => colorScale(d.key))
      .text(d => d.key),
    update => update
      .transition()
      .duration(transitionDuration)
      .attr("y", (d, i) => 16 + i * 16)
      .style("fill", d => colorScale(d.key))
      .text(d => d.key),
    exit => exit
      .transition()
      .duration(transitionDuration/2)
      .style("opacity", 0)
      .remove()
  );

  // Add title for lowest group with appropriate vertical offset
  let lowestTitle = legend.selectAll(".lowest-title").data([0]);
  lowestTitle = lowestTitle.join(
    enter => enter.append("text")
      .attr("class", "lowest-title")
      .attr("y", 16 + 3 * 16 + 16) // After top three items + spacing
      .style("font-size", "11px")
      .style("font-weight", "bold")
      .attr("fill", axisColor)
      .text(`Lowest resale price from ${fromDate} to  ${toDate}`),
    update => update
      .attr("y", 16 + 3 * 16 + 16)
      .text(`Lowest resale price from ${fromDate} to  ${toDate}`)
  );

  // Add bottom three items
  const bottomThreeItems = legend.selectAll(".bottom-item")
    .data(bottomThree);

  bottomThreeItems.join(
    enter => enter.append("text")
      .attr("class", d => `bottom-item item-${makeSafeKey(d.key)}`)
      .attr("y", (d, i) => 16 + 3 * 16 + 16 + 16 + i * 16) // Position after lowest title
      .style("font-size", "10px")
      .style("fill", d => colorScale(d.key))
      .text(d => d.key),
    update => update
      .transition()
      .duration(transitionDuration)
      .attr("y", (d, i) => 16 + 3 * 16 + 16 + 16 + i * 16)
      .style("fill", d => colorScale(d.key))
      .text(d => d.key),
    exit => exit
      .transition()
      .duration(transitionDuration/2)
      .style("opacity", 0)
      .remove()
  );

  const yExtent = d3.extent(data, d => d[y]);
  const yBuffer = (yExtent[1] - yExtent[0]) * 0.05; // 5% buffer
  
  const yScale = d3.scaleLinear()
    //.domain([yExtent[0] - yBuffer, yExtent[1] + yBuffer])
    .domain(groupBy === 'town' ? [100000, 1100000] : (selectedFlatType === '5 ROOM' || selectedFlatType === 'EXECUTIVE') ? [yExtent[0] - yBuffer, yExtent[1] + yBuffer] : [200000, 1300000])
    .range([rect.height - margin.bottom, margin.top]);

  // Update or create Y-axis 
  let yAxis = group.selectAll(".y-axis").data([0]);
  yAxis = yAxis.join(
    enter => enter.append("g")
      .attr("class", "y-axis")
      .attr("transform", `translate(${margin.left},0)`)
  );
  
  yAxis
    .transition()
    .duration(transitionDuration)
    .attr("transform", `translate(${margin.left},0)`)
    .call(d3.axisLeft(yScale).tickSize(-rect.width + margin.right).ticks(4).tickFormat(d3.format("~s")))
    .call(g => {
      g.selectAll("line")
        .attr('stroke', '#525252')
        .attr('stroke-width', 0.7)
        .attr('opacity', 0.3);

      g.selectAll("text")
        .attr('fill', axisColor)
        .style('font-size', '10px');
      g.select(".domain").remove();
    });
    
  res_nested.forEach(type => {
    // Filter out data points with undefined values
    const validDataPoints = type.values.filter(d => 
      d[y] !== null && d[y] !== undefined && !isNaN(d[y])
    );
  
    // At the start of the function, get all existing series
    const existingDotGroups = group.selectAll(".dot")
      .nodes()
      .map(node => node.classList[1])
      .filter(Boolean);

    // After processing new data
    const newSeriesKeys = res_nested.map(d => `dot-${makeSafeKey(d.key)}`);

    // Find classes that exist but aren't in new data
    const removedSeries = existingDotGroups.filter(cls => !newSeriesKeys.includes(cls));

    // Remove those series
    removedSeries.forEach(cls => {
      group.selectAll(`.${cls}`).remove();
      group.selectAll(`.text-${cls.replace('dot-', '')}`).remove();
    });

    // Update dots using join pattern
    const safeKey = makeSafeKey(type.key)
    const dots = group.selectAll(`.dot-${safeKey}`)
      .data(validDataPoints, d => d.date);
    
    dots.join(
      enter => enter.append("circle")
        .attr("class", `dot dot-${safeKey}`)
        .attr("cx", d => xScale(d.date))
        .attr("cy", d => yScale(d[y]))
        .attr("r", 0)
        .style("fill", highlighted.has(type.key) ? colorScale(type.key) : axisColor)
        .style("opacity", 0)
        .style("stroke", "none")
        .transition()
        .duration(transitionDuration)
        .attr("r", highlighted.has(type.key) ? 3 : 2)
        .style("opacity", highlighted.has(type.key) ? 0.8 : 0.3),
      update => update
        .transition()
        .duration(transitionDuration)
        .attr("cx", d => xScale(d.date))
        .attr("cy", d => yScale(d[y]))
        .attr("r", highlighted.has(type.key) ? 3 : 2)
        .style("fill", highlighted.has(type.key) ? colorScale(safeKey) : axisColor)
        .style("opacity", highlighted.has(type.key) ? 0.8 : 0.3),
      exit => exit
        .transition()
        .duration(transitionDuration / 2)
        .attr("r", 0)
        .style("opacity", 0)
        .remove()
    );
  });

  let tooltip = group.selectAll(".tooltip-group").data([0]);
  tooltip = tooltip.join(
    enter => {
      const g = enter.append("g")
        .attr("class", "tooltip-group")
        .style("pointer-events", "none")  // Make sure tooltip doesn't interfere with mouse events
        .style("opacity", 0);             // Start hidden
      
      // Add background rectangle
      g.append("rect")
        .attr("class", "tooltip-bg")
        .attr("rx", 4)                    // Rounded corners
        .attr("ry", 4)
        .attr("width", 0)                 // Will be sized dynamically
        .attr("height", 0)
        .attr("fill", "#333")
        .attr("opacity", 1);
      
      // Add name text (title)
      g.append("text")
        .attr("class", "tooltip-title")
        .attr("x", 8)
        .attr("y", 15)
        .attr("fill", "white")
        .style("font-weight", "bold")
        .style("font-size", "12px");
      
      // Add price text
      g.append("text")
        .attr("class", "tooltip-price")
        .attr("x", 8)
        .attr("y", 32)
        .attr("fill", "white")
        .style("font-size", "11px");
      
      // Add date text
      g.append("text")
        .attr("class", "tooltip-date")
        .attr("x", 8)
        .attr("y", 48)
        .attr("fill", "white")
        .style("font-size", "11px");
      
      return g;
    }
  );
  
  // INTERACTION
  // Unified function to update dots appearance based on current state
  function updateDotsAppearance(options = {}) {
    const {
      groupBy,
      hoveredName = null,
      isHoverEffect = false,
      justClickedName = null,
      removeSelection = false,
      hoveredDotData = null
    } = options;
    
    // Get current selections based on groupBy
    const activeSelections = groupBy === 'town' ? selectedTowns : selectedStreets;
    
    // Calculate the effective selections (accounting for any removals)
    let effectiveSelections = [...activeSelections];
    if (removeSelection && justClickedName) {
      effectiveSelections = effectiveSelections.filter(name => name !== justClickedName);
    } else if (justClickedName && !activeSelections.includes(justClickedName)) {
      effectiveSelections.push(justClickedName);
    }
    
    // Update all dots based on their state
    group.selectAll('.dot').each(function(dotData) {
      if (!dotData) return;
      
      const element = d3.select(this);
      const dotName = dotData[groupBy];
      const isHovered = hoveredName && dotName === hoveredName;
      const isSelected = effectiveSelections.includes(dotName);
      
      // Determine appearance
      let fill, opacity, radius;
      
      if (isSelected || isHovered) {
        // Selected or hovered dots get their actual color and full opacity
        fill = highlighted.has(dotName) ? colorScale(dotName) : '#FF00FF';
        opacity = 1;
        
        const baseRadius = highlighted.has(dotName) ? 2.5 : 2;
        radius = baseRadius * 1.35;
      } else if (effectiveSelections.length > 0) {
        // Non-selected dots when there are selections get white with reduced opacity
        fill = 'white';
        opacity = 0.3;
        radius = highlighted.has(dotName) ? 2.5 : 2;
      } else {
        // Default state - no selections active
        fill = highlighted.has(dotName) ? colorScale(dotName) : axisColor;
        opacity = highlighted.has(dotName) ? 0.8 : 0.3;
        radius = highlighted.has(dotName) ? 2.5 : 2;
      }
      
      // Apply the calculated appearance
      element
        .style('fill', fill)
        .style('opacity', opacity)
        .attr('r', radius)
        .classed('selected-dot', isSelected);
    });
    
    // Handle tooltip display
    if (hoveredDotData && isHoverEffect) {
      // For hover effects, show tooltip for the hovered item
      if (hoveredDotData[y] && hoveredDotData.date) {
        showTooltip(hoveredDotData);
      }
    } else if (effectiveSelections.length > 0) {
      // Show tooltip for most recent selected item
      const lastSelectedName = effectiveSelections[effectiveSelections.length - 1];
      const mostRecentDot = data
        .filter(item => item[groupBy] === lastSelectedName)
        .sort((a, b) => new Date(b.date) - new Date(a.date))[0];
      
      if (mostRecentDot) {
        showTooltip(mostRecentDot);
      }
    } else {
      // No selections or hovers, hide tooltip
      hideTooltip();
    }
    
    return effectiveSelections;
  }

  // Attach event handlers
  group.selectAll('.dot')
    .on('mouseover.highlight', function(event, d) {
      event.preventDefault();
      event.stopPropagation();
      
      const hoveredName = d[groupBy];
      const activeSelections = groupBy === 'town' ? selectedTowns : selectedStreets;
      
      // Only highlight if not already selected
      if (!activeSelections.includes(hoveredName)) {
        updateDotsAppearance({
          groupBy,
          hoveredName,
          isHoverEffect: true,
          hoveredDotData: d
        });
      }
    })
    .on('mouseout.highlight', function(event) {
      event.preventDefault();
      event.stopPropagation();
      
      updateDotsAppearance({ groupBy });
    })
    .on('click.highlight', function(event, d) {
      event.preventDefault();
      event.stopPropagation();
      
      const clickedName = d[groupBy];
      const highlightLayerId = clickedName + '-highlight';
      const activeSelections = groupBy === 'town' ? selectedTowns : selectedStreets;

      // Check if this dot is already selected
      const isAlreadySelected = activeSelections.includes(clickedName);
      
      if (isAlreadySelected) {
        // Unselect this dot
        if (groupBy === 'town') {
          d3.select(".towns-dropdown")
            .selectAll("input[type='checkbox']")
            .filter(function() {
              return d3.select(this.parentNode).attr("data-value") === clickedName;
            })
            .property("checked", false);
          
          selectedTowns = selectedTowns.filter(el => el !== clickedName);
        } else {
          selectedStreets = selectedStreets.filter(el => el !== clickedName);
        }
        
        // Update appearance and remove highlight layer
        updateDotsAppearance({
          groupBy,
          justClickedName: clickedName, 
          removeSelection: true
        });
        removeHighlightLayer(highlightLayerId);
      } else {
        // Add new selection
        if (groupBy === 'town') {
          selectedTowns.push(clickedName);
        } else {
          selectedStreets.push(clickedName);
        }
        
        d3.select(".towns-dropdown")
          .selectAll("input[type='checkbox']")
          .filter(function() {
            return d3.select(this.parentNode).attr("data-value") === clickedName;
          })
          .property("checked", true);
      
        updateDotsAppearance({
          groupBy,
          justClickedName: clickedName
        });
        
        const points = GLOBAL_DATA.filter(point => point[groupBy].includes(clickedName));
        addHighlightLayer(points, highlightLayerId, groupBy === 'street' ? 'black' : '#fff');
      }

      updateClusterButtonState()
      
      updateTownsDropdownLabel(selectedTowns)
    });

  // Show tooltip for a specific data point
  function showTooltip(d) {
    const hoveredName = d[groupBy];
    const hoveredPrice = d[y] || 0;
    const hoveredDate = new Date(d.date);
    
    // Format the price
    const formattedPrice = hoveredPrice.toLocaleString(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits: 0
    });
    
    // Format the date
    const formattedDate = hoveredDate.toLocaleDateString(undefined, {
      year: 'numeric',
      month: 'short',
    });
    
    // Update tooltip text
    tooltip.select(".tooltip-title").text(hoveredName);
    tooltip.select(".tooltip-price").text(`Price: $${formattedPrice}`);
    tooltip.select(".tooltip-date").text(`Date: ${groupBy === 'town' ? d['quarter'] : formattedDate}`);
    
    // Calculate tooltip width based on text
    const titleWidth = tooltip.select(".tooltip-title").node().getComputedTextLength();
    const priceWidth = tooltip.select(".tooltip-price").node().getComputedTextLength();
    const dateWidth = tooltip.select(".tooltip-date").node().getComputedTextLength();
    const tooltipWidth = Math.max(titleWidth, priceWidth, dateWidth) + 16;
    const tooltipHeight = 55;
    
    // Update background rectangle size
    tooltip.select(".tooltip-bg")
      .attr("width", tooltipWidth)
      .attr("height", tooltipHeight);
    
    // Position tooltip above the dot
    const tooltipX = xScale(d.date);
    const tooltipY = yScale(d[y]) - tooltipHeight - 10;
    
    // Adjust if tooltip would go outside chart bounds
    const adjustedX = Math.min(rect.width - margin.right - tooltipWidth, 
                    Math.max(margin.left, tooltipX - tooltipWidth / 2));
    const adjustedY = Math.max(margin.top, tooltipY);
    
    // Position and show tooltip
    tooltip
      .attr("transform", `translate(${adjustedX}, ${adjustedY})`)
      .transition()
      .duration(200)
      .style("opacity", 1);
  }
  
  // Hide tooltip
  function hideTooltip() {
    tooltip
      .transition()
      .duration(200)
      .style("opacity", 0);
  }

  window.highlightDots = updateDotsAppearance
}
///////////////////////////////////////////////////////////////////////////
///////////////////////////////// Miscellaneous ///////////////////////////
///////////////////////////////////////////////////////////////////////////
function createDropdownUI(labelText, items, defaultLabel, onItemClick) {
  // Create main wrapper
  const dropdownWrapper = document.createElement('div');
  dropdownWrapper.className = 'dropdown-wrapper';
  
  // Create label
  const label = document.createElement('label');
  label.textContent = labelText;
  
  // Create dropdown container
  const dropdownContainer = document.createElement('div');
  dropdownContainer.className = 'dropdown-container';
  dropdownContainer.id = 'dropdown';
  
  // Create dropdown button
  const dropdownButton = document.createElement('button');
  dropdownButton.className = 'dropdown-button';
  dropdownButton.textContent = defaultLabel;
  
  // Create dropdown menu
  const dropdownMenu = document.createElement('div');
  dropdownMenu.className = 'dropdown-menu';
  
  // Add chevron
  const buttonText = dropdownButton.textContent;
  dropdownButton.innerHTML = '';
  
  const textSpan = document.createElement('span');
  textSpan.textContent = buttonText;
  
  const chevron = document.createElement('span');
  chevron.className = 'chevron';
  chevron.innerHTML = '&#9662;'; // Unicode down triangle
  
  dropdownButton.appendChild(textSpan);
  dropdownButton.appendChild(chevron);

  // Create dropdown items with optional checkboxes
  items.forEach(item => {
    const dropdownItem = document.createElement('div');
    dropdownItem.className = 'dropdown-item';
    dropdownItem.setAttribute('data-value', item.value);
    
    // If this item should have a checkbox
    if (item.hasCheckbox) {
      const checkbox = document.createElement('input');
      checkbox.type = 'checkbox';
      checkbox.className = 'dropdown-checkbox';
      checkbox.checked = item.checked || false;
      
      const itemText = document.createElement('span');
      itemText.textContent = item.text;
      
      dropdownItem.appendChild(checkbox);
      dropdownItem.appendChild(itemText);
      
      // Prevent checkbox clicks from closing dropdown
      checkbox.addEventListener('click', (e) => {
        e.stopPropagation();
      });
    } else {
      dropdownItem.textContent = item.text;
    }
    
    // Add custom click event
    dropdownItem.addEventListener('click', function(e) {
      e.preventDefault();
      
      // Get checkbox if it exists
      const checkbox = this.querySelector('.dropdown-checkbox');
      if (checkbox) {
        // Toggle checkbox unless the checkbox itself was clicked
        if (!e.target.classList.contains('dropdown-checkbox')) {
          checkbox.checked = !checkbox.checked;
        }
        
        // Call custom click handler with checkbox state
        if (onItemClick) {
          onItemClick(item.value, this.textContent, checkbox ? checkbox.checked : undefined);
        }
        
        // Don't close dropdown when clicking items with checkboxes
        e.stopPropagation();
      } else {
        // For non-checkbox items, update button text
        textSpan.textContent = this.textContent;
        
        // Close dropdown
        dropdownContainer.classList.remove('dropdown-open');
        dropdownButton.classList.remove('active');
        
        // Call custom click handler
        if (onItemClick) {
          onItemClick(item.value, this.textContent);
        }
        
        e.stopPropagation();
      }
    });
    
    dropdownMenu.appendChild(dropdownItem);
  });
  
  // Toggle dropdown on button click
  dropdownButton.addEventListener('click', function(e) {
    e.preventDefault();
    e.stopPropagation();
    dropdownContainer.classList.toggle('dropdown-open');
    dropdownButton.classList.toggle('active');
  });
  
  // Close dropdown when clicking outside
  document.addEventListener('click', function(event) {
    const dropdown = document.getElementById('dropdown');
    if (dropdown && dropdown.classList.contains('dropdown-open') && 
        !dropdown.contains(event.target) && 
        event.target !== dropdownButton) {
      dropdown.classList.remove('dropdown-open');
      dropdownButton.classList.remove('active');
    }
  });
  
  // Assemble the components
  dropdownContainer.appendChild(dropdownButton);
  dropdownContainer.appendChild(dropdownMenu);
  
  dropdownWrapper.appendChild(label);
  dropdownWrapper.appendChild(dropdownContainer);
  
  return dropdownWrapper;
}

function createColorDropdown() {
  const colorOptions = [
    { value: 'type', text: 'Property Type' },
    { value: 'age', text: 'Age' },
    { value: 'price', text: 'Median Resale Price (past 6 months)' }
  ];

  const dropdownWrapper = createDropdownUI('Color by: ', colorOptions, colorOptions.find(d => d.value === selectedLegendStatus).text, (value) => {
    selectedLegendStatus = value;
    updateColorLegend(value);
    updateClusterButtonState()
  })
  document.getElementById('map-color-dropdown').appendChild(dropdownWrapper);

  // Initialize legend with default selection
  updateColorLegend('type');
}

function updateColorLegend(colorBy) {
  // Clear existing legend
  const legendPills = document.querySelector('#color-legend');
  legendPills.innerHTML = '<label>Legend: </label>';

  if (colorBy === 'age') {
    // Update map to color by age
    map.setPaintProperty('circle', 'circle-color', ['get', 'ageColor']);

    // Create age legend
    const ageRanges = [0, 10, 20, 30, 40, 50];
    ageRanges.forEach(age => {
      const pill = document.createElement('div');
      pill.className = 'legend-pill';
      pill.style.backgroundColor = colorAge(age);
      pill.style.color = age <= 20 ? 'black' : 'white';
      pill.textContent = age;
      legendPills.appendChild(pill);
    });
  } 
  if (colorBy === 'type') {
    // Update map to color by property type
    map.setPaintProperty('circle', 'circle-color', ['get', 'color']);

    // Create property type legend
    tags.forEach(type => {
      const pill = document.createElement('div');
      pill.className = 'legend-pill';
      pill.style.backgroundColor = color(type);
      pill.style.color = 'black';
      pill.textContent = type;
      legendPills.appendChild(pill);
    });
  }
  if (colorBy === 'price') {
    // Update map to color by property type
    map.setPaintProperty('circle', 'circle-color', ['get', 'priceColor']);

    const ranges = createRangeLabels(thresholds)

    // Create property type legend
    thresholds.forEach((type,i) => {
      const pill = document.createElement('div');
      pill.className = 'legend-pill';
      pill.style.backgroundColor = colorPrice(type);
      pill.style.color = 'black';
      pill.textContent = ranges[i];
      legendPills.appendChild(pill);
    });
  }
}

function createFlatTypeDropdown() {
  const titleContainer = document.createElement('div');
  titleContainer.className = 'flattype-dropdown';
  titleContainer.style = 'display: flex';

  // Insert the title container before the chart
  const chartContainer = document.getElementById('chart-resale-prices');
  chartContainer.parentNode.insertBefore(titleContainer, chartContainer);

  // Add the first part of the title
  const titlePrefix = document.createElement('span');
  titlePrefix.textContent = 'Median Resale Prices of ';
  titlePrefix.style = 'padding: 5px; font-size: 12px;';
  titleContainer.appendChild(titlePrefix);
  
  // Add the second part of the title
  const titleSuffix = document.createElement('span');
  titleSuffix.id = 'flattype-dropdown-label2'
  titleSuffix.textContent = ' HDB Residential Properties by Town';
  titleSuffix.style = 'padding: 5px; font-size: 12px;';
  
  const flatTypeOptions = [...new Set(GLOBAL_AGG_PRICES_DATA.map(item => item.flat_type))].filter(d => d !== '1 ROOM').sort().map(d => ({ text: d, value: d }));
  
  const dropdownWrapper = createDropdownUI('', flatTypeOptions, selectedFlatType, (value) => {
    selectedFlatType = value; // global store for selection

    // Reset street selection whenever a new flat type is selected
    selectedStreets.forEach(d => {
      removeHighlightLayer(d + '-highlight')
    })
    selectedStreets = []
    
    if(selectedTowns.length === 0 || selectedTowns[0] === 'All Towns' || chartType === 'town') {
      const filteredData = GLOBAL_AGG_PRICES_DATA.filter(item => item.flat_type === value);
      multipleLineChart(filteredData, { groupBy: 'town'})
    } else {
      const filteredData = GLOBAL_PRICES_DATA.filter(item => item.flat_type === value);
      multipleLineChart(filteredData, { groupBy: 'street'})  
    }

    GLOBAL_DATA.forEach((d, i) => {
      const point = GLOBAL_STREETS_PRICES
        .filter(el => el.flat_type === value)
        .find(el => el['block_street'] === (d['block'] + ' ' + d['street']))
      if(point) {
        d.price = isValidNumber(point.price) ? point.price : "NA"
        d.priceColor = isValidNumber(point.price) ? colorPrice(point.price) : 'gray'
      } else {
        d.price = "NA"
        d.priceColor = 'gray'
      }
    })
    const dataNew = GLOBAL_DATA.filter(d => (d.date >= newRange[0]) & (d.date <= newRange[1]))

    if(selectedTowns.length === 0 || selectedTowns[0] === 'All Towns') {
      // Update elements with the full dataset and new range
      updateMarkersMap(dataNew);
    } else {
      // Filter data based on the selected town(s)
      const selectedData = dataNew.filter(d => selectedTowns.indexOf(d.town) !== -1)
      updateMarkersMap(selectedData);
    }
  })

  titleContainer.appendChild(dropdownWrapper);
  titleContainer.appendChild(titleSuffix);
}

function createTownsDropdown(geojson) {
  const titleContainer = document.createElement('div');
  titleContainer.className = 'towns-dropdown';
  titleContainer.style = 'display: flex';

  const contextContainer = document.getElementById('context');
  contextContainer.parentNode.insertBefore(titleContainer, contextContainer);

  // Add the first part of the title
  const paragraph = document.createElement('p');
  const span = document.createElement('span');
  span.id = 'properties-total';
  span.style.fontWeight = 'bold';

  const textNode = document.createTextNode(' properties in ');
  paragraph.appendChild(span);
  paragraph.appendChild(textNode);
  titleContainer.appendChild(paragraph);

  const towns = ["All Towns", ...new Set(GLOBAL_DATA.map(item => item.town).sort())].map(d => ({ text: d, value: d, hasCheckbox: true, checked: false }));

  const dropdownWrapper = createDropdownUI('', towns, 'All Towns', async(value, text, isChecked) => {
    const prevSelectedTowns = [...selectedTowns]; // Store previous checked values
  
    // Find all the values (towns) currently being selected in checkbox
    if (isChecked) {
      selectedTowns.push(value);
    } else {
      const index = selectedTowns.indexOf(value);
      if (index > -1) {
        selectedTowns.splice(index, 1);
      }
    }

    updateTownsDropdownLabel(selectedTowns) 

    updateClusterButtonState()

    // Show highlight layer for each town, and remove any existing highlight layer for unselected towns
    syncHighlightLayers(prevSelectedTowns, selectedTowns);

    if (selectedTowns.length === 0 || selectedTowns[0] === 'All Towns') {
      // Remove all existing streets selected and corresponding highlight layer on map
      selectedStreets.forEach(d => {
        removeHighlightLayer(d + '-highlight')
      })
      selectedStreets = []

      d3.select('#properties-total').html(GLOBAL_DATA.filter(d => (d.date >= newRange[0]) & (d.date <= newRange[1])).length)

      // Update chart to show aggregated prices of properties for each town
      // Filter based on existing flat type selection
      const filteredData = GLOBAL_AGG_PRICES_DATA.filter(d => d.flat_type === selectedFlatType)
  
      multipleLineChart(filteredData, { groupBy: 'town' })

      // Reset to origin map position
      map.flyTo({
        center: [103.9, 1.35],
        zoom: 12.2
      })
    } else {
      const selectedData = GLOBAL_DATA.filter(d => selectedTowns.indexOf(d.town) !== -1 && (d.date >= newRange[0]) & (d.date <= newRange[1]))
      d3.select('#properties-total').html(selectedData.length)

      // Fetch prices data of past 3 years for towns selected
      const currentDate = new Date();
      const currentYearMonth = currentDate.getFullYear() + '-' + String(currentDate.getMonth() + 1).padStart(2, '0'); //Format as YYYY-MM
      const threeYearsAgo = new Date();
      threeYearsAgo.setFullYear(currentDate.getFullYear() - 3);
      const threeYearsAgoYearMonth = threeYearsAgo.getFullYear() + '-' + String(threeYearsAgo.getMonth() + 1).padStart(2, '0');

      const response = await fetch(`/api/prices?towns=${selectedTowns}&start_date=${threeYearsAgoYearMonth}&end_date=${currentYearMonth}`);
      const data = await response.json();

      GLOBAL_PRICES_DATA = JSON.parse(data.prices)  // Store as global variable so that other filters have access to it
      GLOBAL_PRICES_DATA.forEach(d => {
        d.date = new Date(d.date)
        d.price = isValidNumber(d.price) ? +d.price : null
      })
   
      // Update chart to only show prices of properties for each street within towns selected
      const filteredData = GLOBAL_PRICES_DATA.flat().filter(d => d.flat_type === selectedFlatType)

      multipleLineChart(filteredData, { groupBy: 'street' })

      zoomToCentroid(geojson) // Fly to and zoom into centroid location of towns selected
    }
  })

  titleContainer.appendChild(dropdownWrapper);

  const paragraph1 = document.createElement('p');
  const textBefore = document.createTextNode(' completed between ');
  const span1 = document.createElement('span');
  span1.id = 'years';
  span1.style.fontWeight = 'bold';

  paragraph1.appendChild(textBefore);
  paragraph1.appendChild(span1);
  titleContainer.appendChild(paragraph1);
}

function updateTownsDropdownLabel(checkedItems) {
  const textSpan = document.querySelector('.towns-dropdown .dropdown-button span:not(.chevron)');
  const titleSuffix = document.querySelector('#flattype-dropdown-label2')

  if (checkedItems.length === 0) {
    textSpan.textContent = 'All Towns';
    titleSuffix.textContent = ' HDB Residential Properties by Town'
  } else if (checkedItems.length === 1) {
    textSpan.textContent = checkedItems[0];
    titleSuffix.textContent = ' HDB Residential Properties by Street'
  } else {
    textSpan.textContent = checkedItems.map(item => item).join(', ');
    titleSuffix.textContent = ' HDB Residential Properties by Street'
  }
}

function createMapMisc() {
  // Create main container
  const container = document.createElement('div');
  container.style.display = 'flex';
  
  // Create pitch button
  const pitchButton = document.createElement('div');
  pitchButton.className = 'button'
  pitchButton.id = 'pitch-button';
  pitchButton.textContent = 'Display in 3D';
  
  // Create cluster button
  const clusterButton = document.createElement('div');
  clusterButton.className = 'button'
  clusterButton.id = 'cluster-button';
  clusterButton.textContent = 'Enable Clustering';
  
  // Create description paragraph
  const description = document.createElement('p');
  description.style.fontSize = '10px';
  description.style.paddingTop = '10px';
  description.textContent = 'Click on marker to show timeline of average resale price of all properties within the same street';
  
  // Assemble components
  container.appendChild(pitchButton);
  container.appendChild(clusterButton);
  container.appendChild(description);
  
  // Insert into target div
  const mapMisc = document.getElementById('map-misc');
  mapMisc.appendChild(container);

  pitchButton.addEventListener('click', () => {
    if (map.getPitch() === 0) {
        // Change to 3D view
        map.easeTo({ pitch: 60, duration: 1000, zoom: 14.6 });
        pitchButton.textContent = 'Display in 2D';
        tags.forEach(tag => {
          map.setLayoutProperty(tag + '-icon', 'visibility', 'visible');
        })
        map.setLayoutProperty('circle', 'visibility', 'none');
    } else {
        // Change to 2D view
        map.easeTo({ pitch: 0, duration: 1000, zoom: 12.2 });
        pitchButton.textContent = 'Display in 3D';
        tags.forEach(tag => {
          map.setLayoutProperty(tag + '-icon', 'visibility', 'none');
        })
        map.setLayoutProperty('circle', 'visibility', 'visible');
    }
  });

  clusterButton.addEventListener('click', () => {
    const showClusters = (selectedTowns.length === 0 || selectedTowns[0] === "All Towns") && selectedLegendStatus === 'type' && map.getPitch() === 0
    if (showClusters) {
      const newClusterState = toggleClustering(map);
      clusterButton.textContent = newClusterState ? 'Disable Clustering' : 'Enable Clustering'
    }
  }) 

  map.on('pitch', updateClusterButtonState);
}

function updateClusterButtonState() {
  const clusterButton = document.getElementById('cluster-button')
  const showClusters = (selectedTowns.length === 0 || selectedTowns[0] === "All Towns") && selectedLegendStatus === 'type' && map.getPitch() === 0;
  
  if (showClusters) {
    clusterButton.classList.remove('button-disabled');
    clusterButton.style.cursor = 'pointer';
    clusterButton.style.opacity = '1';
  } else {
    clusterButton.classList.add('button-disabled');
    clusterButton.style.cursor = 'not-allowed';
    clusterButton.style.opacity = '0.5';
  }
} 

///////////////////////////////////////////////////////////////////////////
///////////////////////////// Helper functions ////////////////////////////
///////////////////////////////////////////////////////////////////////////
function makeSafeKey(key) {
  return key.replaceAll('/', '-').replaceAll(' ', '-').replaceAll(/[^a-zA-Z0-9\/ ]/g, "-");
}

function parseQuarter(quarterString) {
  // Extract the year (first 4 characters) 
  const year = +quarterString.slice(0, 4);  // Convert the year part to a number
  
  // Extract the quarter number (the character after "Q")
  const quarter = +quarterString.slice(6, 7);  // Get just the number after "Q"
  
  // Map the quarter number to the last month of that quarter
  // Q1 = March (2), Q2 = June (5), Q3 = Sept (8), Q4 = Dec (11)
  const month = ((quarter - 1) * 3) + 2;  // Add 2 to get to the last month of each quarter
  
  // Create a new date object using the year and calculated month (months are 0-based in JS)
  return new Date(year, month, 1);  // The first day of the end month of the quarter
}

// Extract the first numerical value and any alphabets after it
function extractFirstNumber(str) {
  // Match one or more digits followed by zero or more letters
  const match = str.match(/\d+[A-Za-z]*/);
  
  // Return the first match or null if no match found
  return match ? match[0] : null;
}

function createRangeLabels(thresholds) {
  function formatNumber(num) {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
      return (num / 1000).toFixed(0) + 'K';
    }
    return num.toString();
  }
  
  const labels = [];
  
  // Create labels for each range
  for (let i = 0; i < thresholds.length - 1; i++) {
    labels.push(`${formatNumber(thresholds[i])} - ${formatNumber(thresholds[i+1])}`);
  }
  
  labels.push(`Above ${formatNumber(thresholds[thresholds.length - 1])}`);
  
  return labels;
}

function isValidNumber(value) {
  return value !== null && 
         value !== undefined && 
         value !== "NaN" &&
         !isNaN(value);
}


init()