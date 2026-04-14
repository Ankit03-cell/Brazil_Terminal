import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from analysis import (
    get_all_calendar_spreads, 
    get_spreads_on_date, 
    calculate_generic_history, 
    get_fly_curve_on_date, 
    calculate_generic_fly_history, 
    get_index_history
)

st.set_page_config(page_title="Terminal", page_icon="🛡️", layout="wide", initial_sidebar_state="expanded")

# --- CUSTOM CSS ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    
    /* Cards */
    .terminal-card { 
        background-color: #162137; 
        border-radius: 10px; 
        padding: 20px 24px; 
        border: 1px solid #1e3a5f; 
        margin-bottom: 20px;
    }
    .terminal-card:hover { border-color: #2563eb; }
    
    .card-title {
        font-size: 12px; font-weight: 700; letter-spacing: 1.5px; color: #94a3b8; 
        text-transform: uppercase; margin-bottom: 12px; 
        display: flex; align-items: center; gap: 8px;
    }
    .card-title .accent { color: #00e676; font-size: 14px; }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        background-color: #0d1a2d !important; gap: 0px; 
        border-bottom: 2px solid #1e3a5f !important;
        border-radius: 8px 8px 0 0; padding: 4px 4px 0 4px;
    }
    .stTabs [data-baseweb="tab"] { 
        background-color: transparent !important; font-weight: 600; 
        color: #64748b !important; border: none !important; 
        font-size: 13px; letter-spacing: 0.5px;
        padding: 12px 20px !important;
        transition: color 0.2s ease;
    }
    .stTabs [data-baseweb="tab"]:hover { color: #94a3b8 !important; }
    .stTabs [data-baseweb="tab"][aria-selected="true"] { 
        color: #00e676 !important; 
        border-bottom: 3px solid #00e676 !important; font-weight: 700;
    }
    
    /* Metrics */
    div[data-testid="stMetric"] { 
        background: linear-gradient(135deg, #162137 0%, #1a2744 100%);
        border-radius: 8px; padding: 12px 16px !important; 
        border: 1px solid #1e3a5f; 
    }
    div[data-testid="stMetricLabel"] { color: #64748b !important; font-size: 12px !important; text-transform: uppercase; letter-spacing: 0.5px; }
    div[data-testid="stMetricValue"] { color: #00e676 !important; font-weight: 700 !important; font-size: 22px !important; }
    
    /* Sidebar */
    section[data-testid="stSidebar"] { background-color: #0a1628 !important; border-right: 1px solid #1e3a5f; }
    
    /* Selectbox / Inputs */
    div[data-baseweb="select"] > div { background-color: #0d1a2d !important; border-color: #1e3a5f !important; color: #e2e8f0 !important; }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;} footer {visibility: hidden;}
    
    /* Toggle */
    div[data-testid="stToggle"] label span { color: #94a3b8 !important; font-size: 13px; }
    </style>
""", unsafe_allow_html=True)

engine = create_engine('sqlite:///trading_data.db')

@st.cache_data
def load_data():
    df = pd.read_sql("SELECT * FROM market_prices", engine)
    df['date'] = pd.to_datetime(df['date'])
    return df

# --- CHART HELPERS ---
CHART_LAYOUT = dict(
    height=420, hovermode="x unified",
    paper_bgcolor='#162137', plot_bgcolor='#0d1a2d',
    margin=dict(l=50, r=50, t=30, b=50), showlegend=True,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, 
                font=dict(size=11, color='#94a3b8'), bgcolor='rgba(0,0,0,0)'),
    font=dict(family="Inter", size=11, color='#94a3b8'),
)
GRID_STYLE = dict(showgrid=True, gridcolor='#1e3a5f', gridwidth=1, griddash='dot', tickfont=dict(color='#64748b', size=10))
XAXIS_STYLE = dict(showgrid=False, tickfont=dict(color='#64748b', size=10), linecolor='#1e3a5f')

PAST_COLORS = ['#334155', '#475569', '#64748b']
TODAY_LINE = dict(color='#00e676', width=3)
TODAY_MARKER = dict(size=7, color='#00e676', line=dict(width=1, color='#0d1a2d'))

try:
    df_raw = load_data()
    s12 = get_all_calendar_spreads(df_raw, tenor_years=1)
    s24 = get_all_calendar_spreads(df_raw, tenor_years=2)

    # --- SIDEBAR ---
    with st.sidebar:
        st.markdown("""
            <div style='text-align:center; padding: 20px 0 30px 0;'>
                <span style='font-size: 32px;'>🛡️</span>
                <h1 style='margin: 5px 0 0 0; font-size: 22px; font-weight: 700; color: #e2e8f0; letter-spacing: 2px;'>ARB TERMINAL</h1>
                <p style='margin: 0; font-size: 11px; color: #64748b; letter-spacing: 1px;'>MARKET INTELLIGENCE</p>
            </div>
        """, unsafe_allow_html=True)
        st.markdown("---")
        target_date = st.date_input("📅 Target Date", value=df_raw['date'].max().date())
        idx_ticker = st.text_input("📊 Policy Index", value="BZDIOVRA Index")
        st.markdown("---")
        lookback = st.number_input("🔍 Lookback (days)", value=252, min_value=1)
        col1, col2, col3 = st.columns(3)
        with col1: n1 = st.number_input("n1", value=5, min_value=1)
        with col2: n2 = st.number_input("n2", value=21, min_value=1)
        with col3: n3 = st.number_input("n3", value=63, min_value=1)

    # --- RENDERING: SPREAD STATION ---
    def render_spread_station(all_spreads, df_raw, label, target_date, lookback, idx_ticker, n1, n2, n3):
        snap_curr = get_spreads_on_date(all_spreads, target_date)
        
        # Find previous business day snapshot for change bars
        snap_prev = pd.DataFrame()
        for i in range(1, 10):
            test_prev = pd.to_datetime(target_date) - pd.Timedelta(days=i)
            snap_prev = get_spreads_on_date(all_spreads, test_prev)
            if not snap_prev.empty: break

        # Metrics Row
        if not snap_curr.empty:
            odf_vals = snap_curr[snap_curr['Spread Pair'].str.contains('ODF')]['Value']
            nearest = odf_vals.iloc[0] if not odf_vals.empty else 0
            m1, m2, m3 = st.columns(3)
            m1.metric(f"💎 Nearest {label}", f"{nearest:.1f} bps")
            m2.metric("📈 Max", f"{snap_curr['Value'].max():.1f} bps")
            m3.metric("📉 Nodes", len(snap_curr))

        # --- Curve Chart ---
        st.markdown('<div class="terminal-card">', unsafe_allow_html=True)
        c1, c2 = st.columns([3, 1])
        with c1: st.markdown(f"<div class='card-title'><span class='accent'>|</span> {label} SPREAD CURVE</div>", unsafe_allow_html=True)
        with c2: show_all = st.toggle("Show All Nodes", value=False, key=f"tg_s_{label}")
        
        dates_p = [target_date - pd.Timedelta(days=x) for x in [n3, n2, n1]]
        fig_cv = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.06,
                               subplot_titles=[None, "CHG VS PREV CLOSE"])
        
        for idx, d in enumerate(dates_p):
            df_p = get_spreads_on_date(all_spreads, d)
            if not df_p.empty:
                df_p = df_p if show_all else df_p[df_p['Spread Pair'].str.contains('ODF')]
                fig_cv.add_trace(go.Scatter(
                    x=df_p['Spread Pair'], y=df_p['Value'], name=str(d)[:10], 
                    mode='lines', line=dict(color=PAST_COLORS[idx % 3], width=1.5)
                ), row=1, col=1)
                
        df_c = snap_curr if show_all else snap_curr[snap_curr['Spread Pair'].str.contains('ODF')]
        if not df_c.empty:
            fig_cv.add_trace(go.Scatter(
                x=df_c['Spread Pair'], y=df_c['Value'], name="Today", 
                mode='lines+markers', line=TODAY_LINE, marker=TODAY_MARKER
            ), row=1, col=1)
            
        # Change bars
        if not df_c.empty and not snap_prev.empty:
            df_p_matched = snap_prev if show_all else snap_prev[snap_prev['Spread Pair'].str.contains('ODF')]
            merged = pd.merge(df_c, df_p_matched, on='Spread Pair', suffixes=('_curr', '_prev'))
            merged['Chg'] = merged['Value_curr'] - merged['Value_prev']
            bar_colors = ['#ef4444' if v < 0 else '#22c55e' for v in merged['Chg']]
            fig_cv.add_trace(go.Bar(
                x=merged['Spread Pair'], y=merged['Chg'], name='Change', 
                marker_color=bar_colors, marker_line=dict(width=0)
            ), row=2, col=1)
            
        fig_cv.update_layout(**CHART_LAYOUT)
        fig_cv.update_layout(height=500)
        fig_cv.update_yaxes(**GRID_STYLE, row=1, col=1)
        fig_cv.update_yaxes(showgrid=False, zeroline=True, zerolinecolor='#1e3a5f', zerolinewidth=1, tickfont=dict(color='#64748b', size=10), row=2, col=1)
        fig_cv.update_xaxes(**XAXIS_STYLE, showticklabels=False, row=1, col=1)
        fig_cv.update_xaxes(**XAXIS_STYLE, tickangle=-45, row=2, col=1)
        # Style subtitle
        fig_cv.update_annotations(font=dict(size=10, color='#64748b'))
        
        st.plotly_chart(fig_cv, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # --- Historical Trend Chart ---
        st.markdown('<div class="terminal-card">', unsafe_allow_html=True)
        st.markdown(f"<div class='card-title'><span class='accent'>|</span> HISTORICAL {label} TREND</div>", unsafe_allow_html=True)
        c1, c2 = st.columns([1, 1])
        with c1: mode = st.selectbox("Trend Mode", ["Generic (F-Only)", "Generic (Continuous)", "Specific Pair"], key=f"m_s_{label}")
        with c2:
            if "Generic" in mode: rank = st.selectbox("Bucket", [1, 2, 3, 4, 5, 6], index=0, key=f"r_s_{label}")
            else: sel_p = st.selectbox("Pair", snap_curr['Spread Pair'].tolist() if not snap_curr.empty else [], key=f"p_s_{label}")
        
        if "Generic" in mode: 
            h_data = calculate_generic_history(all_spreads, target_date, lookback, rank=rank, month_filter='F' if "F-Only" in mode else None)
        elif not snap_curr.empty:
            raw = all_spreads[sel_p].reset_index().rename(columns={'index':'date', sel_p:'value'})
            h_data = raw.loc[(raw['date'] >= pd.to_datetime(target_date) - pd.Timedelta(days=lookback)) & (raw['date'] <= pd.to_datetime(target_date))].sort_values('date')
        else: 
            h_data = pd.DataFrame()
        
        idx_data = get_index_history(df_raw, idx_ticker, target_date, lookback)
        fig_hist = make_subplots(specs=[[{"secondary_y": True}]])
        if not h_data.empty: 
            fig_hist.add_trace(go.Scatter(
                x=h_data['date'], y=h_data['value'], name="Spread", 
                line=dict(color='#00e676', width=2), fill='tozeroy', fillcolor='rgba(0, 230, 118, 0.08)'
            ), secondary_y=False)
        if not idx_data.empty: 
            fig_hist.add_trace(go.Scatter(
                x=idx_data['date'], y=idx_data['index_value'], name="Policy Rate", 
                line=dict(color='#fbbf24', width=1.5, dash='dot')
            ), secondary_y=True)
        
        fig_hist.update_layout(**CHART_LAYOUT)
        fig_hist.update_yaxes(**GRID_STYLE, title_text="Spread (bps)", title_font=dict(size=11, color='#64748b'), secondary_y=False)
        fig_hist.update_yaxes(showgrid=False, tickfont=dict(color='#64748b', size=10), title_text="Policy Rate", title_font=dict(size=11, color='#64748b'), secondary_y=True)
        fig_hist.update_xaxes(**XAXIS_STYLE)
        st.plotly_chart(fig_hist, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # --- RENDERING: FLY STATION ---
    def render_fly_station(all_spreads_ten, df_raw, label, target_date, lookback, idx_ticker, n1, n2, n3):
        fly_tenor = 2 if "24M" in label else 1
        
        # --- Fly Curve Chart ---
        st.markdown('<div class="terminal-card">', unsafe_allow_html=True)
        c1, c2 = st.columns([3, 1])
        with c1: st.markdown(f"<div class='card-title'><span class='accent'>|</span> {label} FLY CURVE</div>", unsafe_allow_html=True)
        with c2: show_all_f = st.toggle("Show All Nodes", value=False, key=f"tg_f_{label}")
        
        dates_p = [target_date - pd.Timedelta(days=x) for x in [n3, n2, n1]]
        fig_cv = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.06,
                               subplot_titles=[None, "CHG VS PREV CLOSE"])
        
        for idx, d in enumerate(dates_p):
            df_p = get_fly_curve_on_date(all_spreads_ten, d, fly_tenor=fly_tenor, show_all=show_all_f)
            if not df_p.empty: 
                fig_cv.add_trace(go.Scatter(
                    x=df_p['Fly Name'], y=df_p['Value'], name=str(d)[:10], 
                    mode='lines', line=dict(color=PAST_COLORS[idx % 3], width=1.5)
                ), row=1, col=1)
            
        df_c = get_fly_curve_on_date(all_spreads_ten, target_date, fly_tenor=fly_tenor, show_all=show_all_f)
        if not df_c.empty: 
            fig_cv.add_trace(go.Scatter(
                x=df_c['Fly Name'], y=df_c['Value'], name="Today", 
                mode='lines+markers', line=TODAY_LINE, marker=TODAY_MARKER
            ), row=1, col=1)
        
        # Change bars
        snap_prev = pd.DataFrame()
        for i in range(1, 10):
            test_prev = pd.to_datetime(target_date) - pd.Timedelta(days=i)
            snap_prev = get_fly_curve_on_date(all_spreads_ten, test_prev, fly_tenor=fly_tenor, show_all=show_all_f)
            if not snap_prev.empty: break
            
        if not df_c.empty and not snap_prev.empty:
            merged = pd.merge(df_c, snap_prev, on='Fly Name', suffixes=('_curr', '_prev'))
            merged['Chg'] = merged['Value_curr'] - merged['Value_prev']
            bar_colors = ['#ef4444' if v < 0 else '#22c55e' for v in merged['Chg']]
            fig_cv.add_trace(go.Bar(
                x=merged['Fly Name'], y=merged['Chg'], name='Change', 
                marker_color=bar_colors, marker_line=dict(width=0)
            ), row=2, col=1)
            
        fig_cv.update_layout(**CHART_LAYOUT)
        fig_cv.update_layout(height=500)
        fig_cv.update_yaxes(**GRID_STYLE, row=1, col=1)
        fig_cv.update_yaxes(showgrid=False, zeroline=True, zerolinecolor='#1e3a5f', zerolinewidth=1, tickfont=dict(color='#64748b', size=10), row=2, col=1)
        fig_cv.update_xaxes(**XAXIS_STYLE, showticklabels=False, row=1, col=1)
        fig_cv.update_xaxes(**XAXIS_STYLE, tickangle=-45, row=2, col=1)
        fig_cv.update_annotations(font=dict(size=10, color='#64748b'))
        
        st.plotly_chart(fig_cv, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # --- Historical Fly Trend Chart ---
        st.markdown('<div class="terminal-card">', unsafe_allow_html=True)
        st.markdown(f"<div class='card-title'><span class='accent'>|</span> HISTORICAL {label} FLY TREND</div>", unsafe_allow_html=True)
        c1, c2 = st.columns([1, 1])
        with c1: f_mode = st.selectbox("Trend Mode", ["Generic (F-Only)", "Generic (Continuous)", "Specific Fly"], key=f"m_f_{label}")
        with c2:
            if "Generic" in f_mode: rank = st.selectbox("Bucket", [1, 2, 3, 4, 5, 6], index=0, key=f"r_f_{label}")
            else: sel_fly = st.selectbox("Select Fly Pair", df_c['Fly Name'].tolist() if not df_c.empty else [], key=f"p_f_{label}")
        
        h_data = calculate_generic_fly_history(all_spreads_ten, target_date, lookback, rank=rank if "Generic" in f_mode else 1, fly_tenor=fly_tenor, month_filter='F' if "F-Only" in f_mode else (sel_fly[2] if "Specific" in f_mode else None))
        idx_data = get_index_history(df_raw, idx_ticker, target_date, lookback)
        
        if not h_data.empty:
            m1, m2 = st.columns(2)
            m1.metric("Current Fly", f"{h_data['value'].iloc[-1]:.1f} bps")
            m2.metric("Range (Max / Min)", f"{h_data['value'].max():.1f} / {h_data['value'].min():.1f}")
            
            fig_f_hist = make_subplots(specs=[[{"secondary_y": True}]])
            fig_f_hist.add_trace(go.Scatter(
                x=h_data['date'], y=h_data['value'], name="Fly", 
                line=dict(color='#00e676', width=2), fill='tozeroy', fillcolor='rgba(0, 230, 118, 0.08)'
            ), secondary_y=False)
            if not idx_data.empty: 
                fig_f_hist.add_trace(go.Scatter(
                    x=idx_data['date'], y=idx_data['index_value'], name="Policy Rate", 
                    line=dict(color='#fbbf24', width=1.5, dash='dot')
                ), secondary_y=True)
            
            fig_f_hist.update_layout(**CHART_LAYOUT)
            fig_f_hist.update_yaxes(**GRID_STYLE, title_text="Fly (bps)", title_font=dict(size=11, color='#64748b'), secondary_y=False)
            fig_f_hist.update_yaxes(showgrid=False, tickfont=dict(color='#64748b', size=10), title_text="Policy Rate", title_font=dict(size=11, color='#64748b'), secondary_y=True)
            fig_f_hist.update_xaxes(**XAXIS_STYLE)
            st.plotly_chart(fig_f_hist, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # --- TABS ---
    t1, t2, t3, t4 = st.tabs(["📈 12M SPREADS", "📊 24M SPREADS", "🦋 12M FLY", "🦋 24M FLY"])
    with t1: render_spread_station(s12, df_raw, "12M", target_date, lookback, idx_ticker, n1, n2, n3)
    with t2: render_spread_station(s24, df_raw, "24M", target_date, lookback, idx_ticker, n1, n2, n3)
    with t3: render_fly_station(s12, df_raw, "12M", target_date, lookback, idx_ticker, n1, n2, n3)
    with t4: render_fly_station(s24, df_raw, "24M", target_date, lookback, idx_ticker, n1, n2, n3)

except Exception as e:
    st.error(f"Critical System Error: {e}")