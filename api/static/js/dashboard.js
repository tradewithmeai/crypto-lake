/**
 * Crypto Lake Dashboard
 * Live candlestick chart with real-time WebSocket updates
 */

const API_BASE = window.location.origin + '/api/v1';
const WS_BASE = (window.location.protocol === 'https:' ? 'wss://' : 'ws://') + window.location.host + '/api/v1';

// State
let chart = null;
let candleSeries = null;
let volumeSeries = null;
let ws = null;
let currentExchange = '';
let currentSymbol = '';
let currentTf = '5m';
let currentCandle = null;
let currentCandleTime = null;
let currentVolume = 0;
let lastPrice = null;

// Timeframe in seconds
const TF_SECONDS = { '1m': 60, '5m': 300, '15m': 900, '1h': 3600 };

// ---- Initialization ----

async function init() {
    await loadSymbols();
    setupEventListeners();
    createChart();
    await loadChartData();
    connectWebSocket();
}

// ---- Symbol Loading ----

async function loadSymbols() {
    try {
        const resp = await fetch(API_BASE + '/symbols');
        const data = await resp.json();
        const exchangeSelect = document.getElementById('exchange-select');
        const exchanges = data.exchanges || {};

        exchangeSelect.innerHTML = '';
        for (const [name, symbols] of Object.entries(exchanges)) {
            const opt = document.createElement('option');
            opt.value = name;
            opt.textContent = name.charAt(0).toUpperCase() + name.slice(1);
            exchangeSelect.appendChild(opt);
        }

        // Default to first exchange
        currentExchange = exchangeSelect.value;
        populateSymbols(exchanges[currentExchange] || []);
    } catch (err) {
        console.error('Failed to load symbols:', err);
    }
}

function populateSymbols(symbols) {
    const symbolSelect = document.getElementById('symbol-select');
    symbolSelect.innerHTML = '';
    for (const sym of symbols) {
        const opt = document.createElement('option');
        opt.value = sym;
        opt.textContent = sym;
        symbolSelect.appendChild(opt);
    }
    currentSymbol = symbolSelect.value;
}

// ---- Event Listeners ----

function setupEventListeners() {
    document.getElementById('exchange-select').addEventListener('change', async (e) => {
        currentExchange = e.target.value;
        // Reload symbols for new exchange
        const resp = await fetch(API_BASE + '/symbols');
        const data = await resp.json();
        const symbols = data.exchanges[currentExchange] || [];
        populateSymbols(symbols);
        await loadChartData();
        reconnectWebSocket();
    });

    document.getElementById('symbol-select').addEventListener('change', async (e) => {
        currentSymbol = e.target.value;
        await loadChartData();
        reconnectWebSocket();
    });

    document.querySelectorAll('.tf-btn').forEach(btn => {
        btn.addEventListener('click', async (e) => {
            document.querySelectorAll('.tf-btn').forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
            currentTf = e.target.dataset.tf;
            await loadChartData();
        });
    });
}

// ---- Chart ----

function createChart() {
    const container = document.getElementById('chart-container');
    container.innerHTML = '';

    chart = LightweightCharts.createChart(container, {
        width: container.clientWidth,
        height: container.clientHeight,
        layout: {
            background: { color: '#131722' },
            textColor: '#d1d4dc',
        },
        grid: {
            vertLines: { color: '#1e222d' },
            horzLines: { color: '#1e222d' },
        },
        crosshair: {
            mode: LightweightCharts.CrosshairMode.Normal,
        },
        rightPriceScale: {
            borderColor: '#2a2e39',
        },
        timeScale: {
            borderColor: '#2a2e39',
            timeVisible: true,
            secondsVisible: false,
        },
    });

    candleSeries = chart.addCandlestickSeries({
        upColor: '#26a69a',
        downColor: '#ef5350',
        borderDownColor: '#ef5350',
        borderUpColor: '#26a69a',
        wickDownColor: '#ef5350',
        wickUpColor: '#26a69a',
    });

    volumeSeries = chart.addHistogramSeries({
        color: '#26a69a',
        priceFormat: { type: 'volume' },
        priceScaleId: '',
    });

    volumeSeries.priceScale().applyOptions({
        scaleMargins: { top: 0.8, bottom: 0 },
    });

    // Resize handler
    window.addEventListener('resize', () => {
        chart.applyOptions({
            width: container.clientWidth,
            height: container.clientHeight,
        });
    });
}

// ---- Data Loading ----

async function loadChartData() {
    if (!currentSymbol) return;

    try {
        const url = `${API_BASE}/bars/${currentSymbol}/latest?tf=${currentTf}&limit=500`;
        const resp = await fetch(url);
        const data = await resp.json();

        if (!data.data || data.data.length === 0) {
            candleSeries.setData([]);
            volumeSeries.setData([]);
            document.getElementById('live-price').textContent = 'No data';
            return;
        }

        // API returns newest first, reverse for chart (oldest first)
        const bars = data.data.reverse();

        const candles = bars.map(bar => ({
            time: Math.floor(new Date(bar.ts).getTime() / 1000),
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
        }));

        const volumes = bars.map(bar => ({
            time: Math.floor(new Date(bar.ts).getTime() / 1000),
            value: bar.volume_base || 0,
            color: bar.close >= bar.open ? 'rgba(38,166,154,0.3)' : 'rgba(239,83,80,0.3)',
        }));

        candleSeries.setData(candles);
        volumeSeries.setData(volumes);

        // Update price display from latest bar
        const latest = bars[bars.length - 1];
        updatePriceDisplay(latest.close, bars.length > 1 ? bars[bars.length - 2].close : latest.open);

        // Reset current candle tracking
        currentCandle = null;
        currentCandleTime = null;
        currentVolume = 0;

        chart.timeScale().fitContent();
    } catch (err) {
        console.error('Failed to load chart data:', err);
    }
}

function updatePriceDisplay(price, prevPrice) {
    const priceEl = document.getElementById('live-price');
    const changeEl = document.getElementById('price-change');

    // Format price based on magnitude
    const decimals = price > 100 ? 2 : price > 1 ? 4 : 6;
    priceEl.textContent = price.toFixed(decimals);

    if (prevPrice) {
        const change = ((price - prevPrice) / prevPrice) * 100;
        const sign = change >= 0 ? '+' : '';
        changeEl.textContent = `${sign}${change.toFixed(2)}%`;
        changeEl.className = change >= 0 ? 'price-up' : 'price-down';
        priceEl.className = change >= 0 ? 'price-up' : 'price-down';
    }

    lastPrice = price;
}

// ---- WebSocket ----

function connectWebSocket() {
    if (ws) {
        ws.close();
        ws = null;
    }

    if (!currentSymbol) return;

    const url = `${WS_BASE}/ws/stream?symbols=${currentSymbol}&types=trade`;
    ws = new WebSocket(url);

    ws.onopen = () => {
        document.getElementById('ws-status').classList.add('connected');
        document.getElementById('ws-status').classList.remove('disconnected');
        console.log('WebSocket connected:', currentSymbol);
    };

    ws.onmessage = (event) => {
        try {
            const trade = JSON.parse(event.data);
            if (trade.price && trade.stream === 'trade') {
                onTradeMessage(trade);
            }
        } catch (err) {
            // skip
        }
    };

    ws.onclose = () => {
        document.getElementById('ws-status').classList.remove('connected');
        document.getElementById('ws-status').classList.add('disconnected');
        // Auto-reconnect after 3 seconds
        setTimeout(() => {
            if (currentSymbol) connectWebSocket();
        }, 3000);
    };

    ws.onerror = () => {
        document.getElementById('ws-status').classList.remove('connected');
        document.getElementById('ws-status').classList.add('disconnected');
    };
}

function reconnectWebSocket() {
    if (ws) {
        ws.close();
        ws = null;
    }
    currentCandle = null;
    currentCandleTime = null;
    currentVolume = 0;
    connectWebSocket();
}

function onTradeMessage(trade) {
    const price = trade.price;
    const qty = trade.qty || 0;
    const tradeTimeSec = Math.floor(trade.ts_event / 1000);
    const tfSec = TF_SECONDS[currentTf] || 60;
    const candleStart = Math.floor(tradeTimeSec / tfSec) * tfSec;

    if (!currentCandle || candleStart > currentCandleTime) {
        // New candle
        currentCandle = { time: candleStart, open: price, high: price, low: price, close: price };
        currentCandleTime = candleStart;
        currentVolume = qty;
    } else {
        // Update existing candle
        currentCandle.high = Math.max(currentCandle.high, price);
        currentCandle.low = Math.min(currentCandle.low, price);
        currentCandle.close = price;
        currentVolume += qty;
    }

    candleSeries.update(currentCandle);
    volumeSeries.update({
        time: candleStart,
        value: currentVolume,
        color: currentCandle.close >= currentCandle.open ? 'rgba(38,166,154,0.3)' : 'rgba(239,83,80,0.3)',
    });

    // Update price display
    updatePriceDisplay(price, lastPrice || price);
}

// ---- Start ----
document.addEventListener('DOMContentLoaded', init);
