import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import json
import threading
import time
from collections import defaultdict
import pyodbc
from alphaCommon import alphaDb
from alphaTrade_optimized import (
    TickerState, Strategy, topMomentumStrategy, topMomentumStrategy2, 
    highGrowthStrategy, specialStrategy, alphaPicksStrategy, mixedStrategy,
    Status, optimize_data_loading
)


class TradingBacktestGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Alpha Trading Backtest GUI")
        self.root.geometry("1400x800")  # Reduced height from 1000 to 800 since layout is more compact
        
        # Data storage
        self.data_by_date = None
        self.current_results = None
        self.strategy_instances = {}
        self.trade_history = []  # Track all trades
        
        # Global variables for backtest
        self.g_startDate = date(2025, 5, 30)
        self.g_endDate = date(2025, 7, 25)
        self.g_startCash = 350000
        self.g_tradeSize = 6000
        self.g_maxPostion = 2000
        self.g_minCash = 20000
        
        # Create main frames
        self.create_widgets()
        
        # Initialize strategy instances
        self.initialize_strategies()
        
    def create_widgets(self):
        # Main container
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Left panel - Controls with scrollbar
        left_panel_container = ttk.Frame(main_frame, width=600)
        left_panel_container.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10))
        
        # Create canvas and scrollbar for left panel
        left_canvas = tk.Canvas(left_panel_container, width=580)
        left_scrollbar = ttk.Scrollbar(left_panel_container, orient=tk.VERTICAL, command=left_canvas.yview)
        left_panel = ttk.Frame(left_canvas)
        
        left_canvas.configure(yscrollcommand=left_scrollbar.set)
        
        # Pack the scrollbar and canvas
        left_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        left_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Create a window in the canvas for the left panel
        left_canvas.create_window((0, 0), window=left_panel, anchor=tk.NW)
        
        # Right panel - Results and Charts
        right_panel = ttk.Frame(main_frame)
        right_panel.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        
        self.create_control_panel(left_panel)
        self.create_results_panel(right_panel)
        
        # Configure scrolling
        left_panel.bind("<Configure>", lambda e: left_canvas.configure(scrollregion=left_canvas.bbox("all")))
        left_canvas.bind("<Configure>", lambda e: left_canvas.itemconfig(left_canvas.find_withtag("window"), width=e.width))
        
    def create_control_panel(self, parent):
        # Title
        title_label = ttk.Label(parent, text="Trading Backtest Controls", font=("Arial", 14, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Quick Start Section
        quick_start_frame = ttk.LabelFrame(parent, text="Quick Start", padding=10)
        quick_start_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(quick_start_frame, text="1. Load Data", font=("Arial", 10, "bold")).pack(anchor=tk.W)
        ttk.Label(quick_start_frame, text="2. Select Strategy", font=("Arial", 10, "bold")).pack(anchor=tk.W)
        ttk.Label(quick_start_frame, text="3. Run Backtest", font=("Arial", 10, "bold")).pack(anchor=tk.W)
        
        # Data Loading Section
        data_frame = ttk.LabelFrame(parent, text="Data Loading", padding=10)
        data_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Date range
        ttk.Label(data_frame, text="Start Date:").pack(anchor=tk.W)
        self.start_date_var = tk.StringVar(value="2025-05-30")
        start_date_entry = ttk.Entry(data_frame, textvariable=self.start_date_var)
        start_date_entry.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(data_frame, text="End Date:").pack(anchor=tk.W)
        self.end_date_var = tk.StringVar(value="2025-07-25")
        end_date_entry = ttk.Entry(data_frame, textvariable=self.end_date_var)
        end_date_entry.pack(fill=tk.X, pady=(0, 10))
        
        # Load data button
        self.load_btn = ttk.Button(data_frame, text="Load Data", command=self.load_data)
        self.load_btn.pack(fill=tk.X, pady=(10, 0))
        
        # Strategy Selection Section
        strategy_frame = ttk.LabelFrame(parent, text="Strategy Selection", padding=10)
        strategy_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(strategy_frame, text="Strategy:").pack(anchor=tk.W)
        self.strategy_var = tk.StringVar(value="topMomentumStrategy")
        strategy_combo = ttk.Combobox(strategy_frame, textvariable=self.strategy_var, state="readonly")
        strategy_combo['values'] = [
            "topMomentumStrategy",
            "topMomentumStrategy2", 
            "highGrowthStrategy",
            "specialStrategy",
            "alphaPicksStrategy",
            "mixedStrategy"
        ]
        strategy_combo.pack(fill=tk.X, pady=(0, 10))
        strategy_combo.bind('<<ComboboxSelected>>', self.on_strategy_change)
        
        # Strategy Parameters Section
        self.params_frame = ttk.LabelFrame(parent, text="Strategy Parameters", padding=10)
        self.params_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Portfolio Parameters Section
        portfolio_frame = ttk.LabelFrame(parent, text="Portfolio Parameters", padding=10)
        portfolio_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Create two rows for portfolio parameters
        portfolio_row1 = ttk.Frame(portfolio_frame)
        portfolio_row1.pack(fill=tk.X, pady=(0, 5))
        
        portfolio_row2 = ttk.Frame(portfolio_frame)
        portfolio_row2.pack(fill=tk.X)
        
        # Initial cash
        ttk.Label(portfolio_row1, text="Initial Cash:").pack(side=tk.LEFT)
        self.initial_cash_var = tk.StringVar(value="350000")
        ttk.Entry(portfolio_row1, textvariable=self.initial_cash_var, width=15).pack(side=tk.LEFT, padx=(5, 15))
        
        # Trade size
        ttk.Label(portfolio_row1, text="Trade Size:").pack(side=tk.LEFT)
        self.trade_size_var = tk.StringVar(value="6000")
        ttk.Entry(portfolio_row1, textvariable=self.trade_size_var, width=15).pack(side=tk.LEFT, padx=(5, 0))
        
        # Max positions
        ttk.Label(portfolio_row2, text="Max Positions:").pack(side=tk.LEFT)
        self.max_positions_var = tk.StringVar(value="2000")
        ttk.Entry(portfolio_row2, textvariable=self.max_positions_var, width=15).pack(side=tk.LEFT, padx=(5, 15))
        
        # Min cash
        ttk.Label(portfolio_row2, text="Min Cash:").pack(side=tk.LEFT)
        self.min_cash_var = tk.StringVar(value="20000")
        ttk.Entry(portfolio_row2, textvariable=self.min_cash_var, width=15).pack(side=tk.LEFT, padx=(5, 0))
        
        # Run Backtest Section
        run_frame = ttk.LabelFrame(parent, text="Run Backtest", padding=10)
        run_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.run_btn = ttk.Button(run_frame, text="Run Backtest", command=self.run_backtest)
        self.run_btn.pack(fill=tk.X)
        
        # Progress bar
        self.progress_var = tk.StringVar(value="Ready")
        ttk.Label(run_frame, textvariable=self.progress_var).pack(pady=(10, 0))
        
        # Comparison Section
        compare_frame = ttk.LabelFrame(parent, text="Strategy Comparison", padding=10)
        compare_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.compare_btn = ttk.Button(compare_frame, text="Compare Strategies", command=self.compare_strategies)
        self.compare_btn.pack(fill=tk.X)
        
        # Export Section
        export_frame = ttk.LabelFrame(parent, text="Export Results", padding=10)
        export_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.export_btn = ttk.Button(export_frame, text="Export Results", command=self.export_results)
        self.export_btn.pack(fill=tk.X)
        
    def create_results_panel(self, parent):
        # Create notebook for tabs
        self.notebook = ttk.Notebook(parent)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # Summary tab
        self.summary_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.summary_frame, text="Summary")
        
        # Create text widget for summary
        self.summary_text = tk.Text(self.summary_frame, wrap=tk.WORD, font=("Courier", 10))
        summary_scrollbar = ttk.Scrollbar(self.summary_frame, orient=tk.VERTICAL, command=self.summary_text.yview)
        self.summary_text.configure(yscrollcommand=summary_scrollbar.set)
        
        self.summary_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        summary_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Performance tab
        self.performance_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.performance_frame, text="Performance")
        
        # Create matplotlib figure for performance chart
        self.fig, self.ax = plt.subplots(figsize=(10, 6))
        self.canvas = FigureCanvasTkAgg(self.fig, self.performance_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # Trades tab
        self.trades_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.trades_frame, text="Trades")
        
        # Create treeview for trades
        self.trades_tree = ttk.Treeview(self.trades_frame, columns=("Date", "Ticker", "Action", "Shares", "Price", "Profit"), show="headings")
        self.trades_tree.heading("Date", text="Date")
        self.trades_tree.heading("Ticker", text="Ticker")
        self.trades_tree.heading("Action", text="Action")
        self.trades_tree.heading("Shares", text="Shares")
        self.trades_tree.heading("Price", text="Price")
        self.trades_tree.heading("Profit", text="Profit")
        
        # Set column widths
        self.trades_tree.column("Date", width=100)
        self.trades_tree.column("Ticker", width=80)
        self.trades_tree.column("Action", width=100)
        self.trades_tree.column("Shares", width=80)
        self.trades_tree.column("Price", width=100)
        self.trades_tree.column("Profit", width=120)
        
        trades_scrollbar = ttk.Scrollbar(self.trades_frame, orient=tk.VERTICAL, command=self.trades_tree.yview)
        self.trades_tree.configure(yscrollcommand=trades_scrollbar.set)
        
        self.trades_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        trades_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Positions tab
        self.positions_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.positions_frame, text="Positions")
        
        # Create treeview for positions
        self.positions_tree = ttk.Treeview(self.positions_frame, columns=("Ticker", "Shares", "AvgPrice", "CurrentPrice", "HoldingDays", "OpenPL", "PLPct"), show="headings")
        self.positions_tree.heading("Ticker", text="Ticker")
        self.positions_tree.heading("Shares", text="Shares")
        self.positions_tree.heading("AvgPrice", text="Avg Price")
        self.positions_tree.heading("CurrentPrice", text="Current Price")
        self.positions_tree.heading("HoldingDays", text="Holding Days")
        self.positions_tree.heading("OpenPL", text="Open P/L")
        self.positions_tree.heading("PLPct", text="P/L %")
        
        # Set column widths
        self.positions_tree.column("Ticker", width=80)
        self.positions_tree.column("Shares", width=80)
        self.positions_tree.column("AvgPrice", width=100)
        self.positions_tree.column("CurrentPrice", width=100)
        self.positions_tree.column("HoldingDays", width=100)
        self.positions_tree.column("OpenPL", width=120)
        self.positions_tree.column("PLPct", width=80)
        
        positions_scrollbar = ttk.Scrollbar(self.positions_frame, orient=tk.VERTICAL, command=self.positions_tree.yview)
        self.positions_tree.configure(yscrollcommand=positions_scrollbar.set)
        
        self.positions_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        positions_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
    def initialize_strategies(self):
        """Initialize all strategy instances with default parameters"""
        self.strategy_instances = {
            "topMomentumStrategy": topMomentumStrategy(),
            "topMomentumStrategy2": topMomentumStrategy2(),
            "highGrowthStrategy": highGrowthStrategy(),
            "specialStrategy": specialStrategy(),
            "alphaPicksStrategy": alphaPicksStrategy(),
            "mixedStrategy": mixedStrategy()
        }
        
        # Create parameter widgets for each strategy
        self.create_strategy_parameters()
        
    def create_strategy_parameters(self):
        """Create parameter input widgets for the current strategy"""
        # Clear existing widgets
        for widget in self.params_frame.winfo_children():
            widget.destroy()
            
        strategy_name = self.strategy_var.get()
        if strategy_name not in self.strategy_instances:
            return
            
        strategy = self.strategy_instances[strategy_name]
        
        # Common parameters
        params = [
            ("quantRating", "Quant Rating", strategy.quantRating),
            ("quantRatingLo", "Quant Rating Low", strategy.quantRatingLo),
            ("sellSideRating", "Sell Side Rating", strategy.sellSideRating),
            ("sellSideRatingLo", "Sell Side Rating Low", strategy.sellSideRatingLo),
            ("authorsRating", "Authors Rating", strategy.authorsRating),
            ("authorRatingLo", "Author Rating Low", strategy.authorRatingLo),
            ("minPrice", "Min Price", strategy.minPrice),
            ("growthGrade", "Growth Grade", strategy.growthGrade),
            ("momentumGrade", "Momentum Grade", strategy.momentumGrade),
            ("profitabilityGrade", "Profitability Grade", strategy.profitabilityGrade),
            ("epsRevisionsGrade", "EPS Revisions Grade", strategy.epsRevisionsGrade),
            ("valueGrade", "Value Grade", strategy.valueGrade),
            ("stopLoss", "Stop Loss", strategy.stopLoss),
            ("takeProfit", "Take Profit", strategy.takeProfit),
            ("trailingStop", "Trailing Stop", strategy.trailingStop),
            ("forceStopLoss", "Force Stop Loss", strategy.forceStopLoss),
            ("useStablizer", "Use Stabilizer", strategy.useStablizer),
            ("addPosition", "Add Position", strategy.addPosition),
            ("addPositionthreshold_lo", "Add Position Threshold Low", strategy.addPositionthreshold_lo),
            ("addPositionthreshold_hi", "Add Position Threshold High", strategy.addPositionthreshold_hi)
        ]
        
        # Create parameter widgets in two columns
        self.param_vars = {}
        
        # Create left and right columns
        left_column = ttk.Frame(self.params_frame)
        left_column.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        
        right_column = ttk.Frame(self.params_frame)
        right_column.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(5, 0))
        
        # Split parameters into two lists
        mid_point = len(params) // 2
        left_params = params[:mid_point]
        right_params = params[mid_point:]
        
        # Create left column parameters
        for param_name, display_name, default_value in left_params:
            param_frame = ttk.Frame(left_column)
            param_frame.pack(fill=tk.X, pady=(0, 5))
            
            ttk.Label(param_frame, text=f"{display_name}:", width=25).pack(side=tk.LEFT)
            var = tk.StringVar(value=str(default_value))
            self.param_vars[param_name] = var
            ttk.Entry(param_frame, textvariable=var, width=20).pack(side=tk.RIGHT)
        
        # Create right column parameters
        for param_name, display_name, default_value in right_params:
            param_frame = ttk.Frame(right_column)
            param_frame.pack(fill=tk.X, pady=(0, 5))
            
            ttk.Label(param_frame, text=f"{display_name}:", width=25).pack(side=tk.LEFT)
            var = tk.StringVar(value=str(default_value))
            self.param_vars[param_name] = var
            ttk.Entry(param_frame, textvariable=var, width=20).pack(side=tk.RIGHT)
            
    def on_strategy_change(self, event=None):
        """Handle strategy selection change"""
        self.create_strategy_parameters()
        
    def load_data(self):
        """Load data from database"""
        def load_data_thread():
            try:
                self.progress_var.set("Loading data...")
                self.load_btn.config(state="disabled")
                
                # Parse dates
                start_date = datetime.strptime(self.start_date_var.get(), "%Y-%m-%d").date()
                end_date = datetime.strptime(self.end_date_var.get(), "%Y-%m-%d").date()
                
                # Update global dates
                self.g_startDate = start_date
                self.g_endDate = end_date
                
                # Load data using custom function with GUI date range
                self.data_by_date = self.load_data_with_date_range(start_date, end_date)
                
                self.progress_var.set(f"Data loaded: {len(self.data_by_date)} trading days")
                self.load_btn.config(state="normal")
                
                messagebox.showinfo("Success", f"Data loaded successfully!\n{len(self.data_by_date)} trading days available.")
                
            except Exception as e:
                self.progress_var.set("Error loading data")
                self.load_btn.config(state="normal")
                messagebox.showerror("Error", f"Failed to load data: {str(e)}")
        
        # Run in separate thread to avoid blocking GUI
        thread = threading.Thread(target=load_data_thread)
        thread.daemon = True
        thread.start()
        
    def load_data_with_date_range(self, start_date, end_date):
        """Load data with specific date range"""
        db = alphaDb()
        
        # Load all data at once with optimized query using GUI date range
        sql = (f"SELECT * From DailyPrice join Ratings on DailyPrice.Ticker=Ratings.Ticker and DailyPrice.Date=Ratings.Date "
                f" where Ratings.Date  >= '{start_date.strftime("%Y-%m-%d")}'  and Ratings.Date  <= '{end_date.strftime("%Y-%m-%d")}' "
                f" ORDER BY Ratings.Date, Ratings.Ticker")
        
        res = db.queryDbSql(sql)
        
        # Convert to pandas DataFrame for faster processing
        data = []
        for row in res:
            data.append({
                'Date': row.Date,
                'Ticker': row.Ticker.rstrip().upper(),
                'Price': row.O,
                'quantRating': row.quantRating,
                'sellSideRating': row.sellSideRating,
                'authorsRating': row.authorsRating,
                'growthGrade': row.growthGrade,
                'momentumGrade': row.momentumGrade,
                'epsRevisionsGrade': row.epsRevisionsGrade,
                'profitabilityGrade': row.profitabilityGrade,
                'valueGrade': row.valueGrade
            })
        
        df = pd.DataFrame(data)
        
        # Group by date for faster lookup
        data_by_date = {}
        for date_str, group in df.groupby('Date'):
            date_key = date_str.strftime("%Y-%m-%d")
            data_by_date[date_key] = group.to_dict('records')
        
        return data_by_date
        
    def run_backtest(self):
        """Run the backtest with current parameters"""
        if self.data_by_date is None:
            messagebox.showerror("Error", "Please load data first!")
            return
            
        def run_backtest_thread():
            try:
                self.progress_var.set("Running backtest...")
                self.run_btn.config(state="disabled")
                
                # Update strategy parameters
                self.update_strategy_parameters()
                
                # Update global variables
                self.g_startCash = float(self.initial_cash_var.get())
                self.g_tradeSize = float(self.trade_size_var.get())
                self.g_maxPostion = int(self.max_positions_var.get())
                self.g_minCash = float(self.min_cash_var.get())
                
                # Run backtest
                self.current_results = self.run_custom_backtest()
                
                # Update GUI
                self.root.after(0, self.update_results_display)
                
                self.progress_var.set("Backtest completed!")
                self.run_btn.config(state="normal")
                
            except Exception as e:
                import traceback
                self.progress_var.set("Error running backtest")
                self.run_btn.config(state="normal")
                error_msg = str(e)
                traceback_info = traceback.format_exc()
                full_error = f"Backtest failed: {error_msg}\n\nFull traceback:\n{traceback_info}"
                print("="*80)
                print("ERROR DETAILS:")
                print("="*80)
                print(full_error)
                print("="*80)
                self.root.after(0, lambda: messagebox.showerror("Error", full_error))
        
        # Run in separate thread
        thread = threading.Thread(target=run_backtest_thread)
        thread.daemon = True
        thread.start()
        
    def update_strategy_parameters(self):
        """Update the current strategy with GUI parameters"""
        strategy_name = self.strategy_var.get()
        strategy = self.strategy_instances[strategy_name]
        
        # Update strategy parameters
        for param_name, var in self.param_vars.items():
            try:
                value = float(var.get())
                setattr(strategy, param_name, value)
            except ValueError:
                # Try as integer for some parameters
                try:
                    value = int(var.get())
                    setattr(strategy, param_name, value)
                except ValueError:
                    pass  # Keep default value
        
        # Update portfolio-specific parameters
        strategy.tradeSize = float(self.trade_size_var.get())
        strategy.maxPostion = int(self.max_positions_var.get())
        strategy.minCash = float(self.min_cash_var.get())
                    
    def run_custom_backtest(self):
        """Run backtest with custom parameters"""
        stat = Status(self.g_startCash)
        strategy = self.strategy_instances[self.strategy_var.get()]
        holdings = {}
        self.trade_history = []  # Reset trade history
        
        # Pre-calculate trading dates
        trading_dates = []
        for single_date in self.daterange(self.g_startDate, self.g_endDate):
            if single_date.weekday() < 5:
                trading_dates.append(single_date)
        
        print(f"Processing {len(trading_dates)} trading days...")
        
        for single_date in trading_dates:
            dateName = single_date.strftime("%Y-%m-%d")
            
            # Fast lookup using pre-grouped data
            if dateName not in self.data_by_date:
                continue
                
            todayRows = self.data_by_date[dateName]
            if len(todayRows) == 0:
                continue
            
            # Pre-build current market state
            currMarket = {}
            action = {}
            
            # Process all tickers for today in one pass
            for row in todayRows:
                ticker = row['Ticker']
                
                # Reuse existing TickerState or create new one
                if ticker in holdings:
                    tickerState = holdings[ticker]
                else:
                    tickerState = TickerState()
                    tickerState.ticker = ticker
                
                # Update ticker state efficiently
                tickerState.price = row['Price']
                tickerState.currentDate = single_date
                tickerState.quantRating = row['quantRating']
                tickerState.sellSideRating = row['sellSideRating']
                tickerState.authorsRating = row['authorsRating']
                tickerState.growthGrade = row['growthGrade']
                tickerState.momentumGrade = row['momentumGrade']
                tickerState.epsRevisionsGrade = row['epsRevisionsGrade']
                tickerState.profitabilityGrade = row['profitabilityGrade']
                tickerState.valueGrade = row['valueGrade']
                
                currMarket[ticker] = tickerState
                
                # Calculate strategy decisions
                tier = strategy.qualified(tickerState)
                action[ticker] = strategy.decision(tickerState, tier)
            
            # Process holdings efficiently
            newHoldings = {}
            posCount = len(holdings)
            
            # Process existing holdings
            for ticker, holding in holdings.items():
                if ticker not in currMarket:
                    # Keep existing position if no market data
                    action[ticker] = Strategy.ACTION.KEEP
                    newHoldings[ticker] = holding
                    continue
                    
                tickerState = currMarket[ticker]
                price = tickerState.price
                rating = tickerState.quantRating
                
                if action[ticker] == Strategy.ACTION.SELL:
                    # Process sell action
                    shares = holding.numShares
                    entryPrice = holding.entryPrice
                    profit = (price - entryPrice) * shares
                    profitPct = (price - entryPrice) / entryPrice * 100
                    
                    print(f" --SOLD {ticker} {shares} shares at {price:.2f} with profit {profit:.2f} {profitPct:.02f}% (rating={rating:.3f}")
                    
                    # Record sell trade
                    self.trade_history.append({
                        'date': single_date,
                        'ticker': ticker,
                        'action': 'SELL',
                        'shares': shares,
                        'price': price,
                        'profit': profit,
                        'rating': rating
                    })
                    
                    delta = single_date - holding.entryDate
                    if profit >= 0:
                        stat.wins += 1
                        stat.winAmt += profit
                        stat.winHoldingDays += delta.days
                    else:
                        stat.loses += 1
                        stat.lossAmt += profit
                        stat.lossHoldingDays += delta.days
                    
                    stat.holdingDays += delta.days
                    stat.tickProfit[ticker] = stat.tickProfit.get(ticker, 0) + profit
                    stat.profit += profit
                    stat.sold += 1
                    posCount -= 1
                    stat.cash += shares * price
                    
                elif action[ticker] == Strategy.ACTION.SCALEOUT:
                    # Process scale out
                    shares = int(holding.numShares / 5)
                    entryPrice = holding.entryPrice
                    profit = (price - entryPrice) * shares
                    profitPct = (price - entryPrice) / entryPrice * 100
                    
                    print(f" --SCALE OUT {ticker} {shares} shares at {price:.2f} with profit {profit:.2f} {profitPct:.02f}% (rating={rating:.3f}")
                    
                    # Record scale out trade
                    self.trade_history.append({
                        'date': single_date,
                        'ticker': ticker,
                        'action': 'SCALE OUT',
                        'shares': shares,
                        'price': price,
                        'profit': profit,
                        'rating': rating
                    })
                    
                    stat.profit += profit
                    stat.cash += shares * price
                    holding.numShares -= shares
                    holding.trades.append({'shares': -shares, 'price': price, 'date': single_date})
                    newHoldings[ticker] = holding
                    
                elif action[ticker] in [Strategy.ACTION.HOLD, Strategy.ACTION.KEEP]:
                    # Update trailing price if needed
                    if action[ticker] == Strategy.ACTION.HOLD and price > holding.trailingPrice:
                        holding.trailingPrice = price
                    newHoldings[ticker] = holding
            
            # Process new positions
            for row in todayRows:
                ticker = row['Ticker']
                
                if action[ticker] not in [Strategy.ACTION.BUY, Strategy.ACTION.ADD]:
                    continue
                    
                tickerState = currMarket[ticker]
                price = tickerState.price
                rating = tickerState.quantRating
                
                if posCount <= strategy.maxPostion and stat.cash > strategy.minCash + strategy.tradeSize:
                    posCount += 1
                    tradeSize = strategy.getTradeSize(ticker, single_date)
                    if action[ticker] == Strategy.ACTION.ADD:
                        tradeSize = tradeSize * 1
                        
                    newShares = int(tradeSize / price)
                    
                    if len(tickerState.trades) == 0:
                        tickerState.numShares = newShares
                        tickerState.entryPrice = price
                        tickerState.entryDate = single_date
                    else:
                        value = newShares * price + tickerState.numShares * tickerState.entryPrice
                        tickerState.numShares += newShares
                        tickerState.entryPrice = value / tickerState.numShares
                    
                    tickerState.trades.append({'shares': newShares, 'price': price, 'date': single_date})
                    newHoldings[ticker] = tickerState
                    stat.added += 1
                    
                    if action[ticker] == Strategy.ACTION.BUY:
                        print(f" ### OPEN {newShares} shares of {ticker} at {price} rating={tickerState.quantRating}, authors={tickerState.authorsRating}, sellSide={tickerState.sellSideRating} momentum={tickerState.momentumGrade}")
                        # Record buy trade
                        self.trade_history.append({
                            'date': single_date,
                            'ticker': ticker,
                            'action': 'BUY',
                            'shares': newShares,
                            'price': price,
                            'profit': 0,
                            'rating': tickerState.quantRating
                        })
                    else:
                        print(f" ### ADD {newShares} shares of {ticker} at {price}")
                        # Record add trade
                        self.trade_history.append({
                            'date': single_date,
                            'ticker': ticker,
                            'action': 'ADD',
                            'shares': newShares,
                            'price': price,
                            'profit': 0,
                            'rating': tickerState.quantRating
                        })
                        
                    stat.cash -= price * newShares
                elif action[ticker] == Strategy.ACTION.ADD:
                    # For ADD actions that don't meet the position/cash criteria, keep existing position
                    if ticker in holdings:
                        newHoldings[ticker] = holdings[ticker]
                elif action[ticker] == Strategy.ACTION.BUY:
                    # For BUY actions that don't meet the position/cash criteria, skip the trade
                    pass
            
            # Update portfolio statistics
            pos = {'Date': single_date, 'Holdings': newHoldings, 'Cash': stat.cash}
            stat.dailyPositions.append(pos)
            holdings = newHoldings
            
            # Calculate current portfolio value
            stat.stockVal = sum(holding.numShares * holding.price for holding in newHoldings.values())
            
            if stat.minCash > stat.cash:
                stat.minCash = stat.cash
                
            wholeVal = stat.cash + stat.stockVal
            if stat.minVal == 0 or stat.minVal > wholeVal:
                stat.minVal = wholeVal
            if stat.maxVal < wholeVal:
                stat.maxVal = wholeVal
                
            if stat.maxVal > stat.minCash:
                drawdown = (stat.maxVal - wholeVal) / (stat.maxVal - stat.minCash)
                realDrawdown = (stat.maxVal - wholeVal) / stat.maxVal
                if stat.maxDrawndown < drawdown:
                    stat.maxDrawndown = drawdown
                if stat.maxRealDrawndown < realDrawdown:
                    stat.maxRealDrawndown = realDrawdown
            
            print(f"{single_date} ==== Number of positions = {len(newHoldings)} Cash {stat.cash:.2f}, Stock {stat.stockVal:.2f} Total {stat.cash+stat.stockVal:.2f}, min cash {stat.minCash:.2f}")
            
            if len(newHoldings) > stat.maxPositions:
                stat.maxPositions = len(newHoldings)
        
        return stat
        
    def daterange(self, start_date, end_date):
        """Generate date range"""
        days = int((end_date - start_date).days)
        for n in range(days):
            yield start_date + timedelta(n)
            
    def update_results_display(self):
        """Update the results display with current backtest results"""
        if self.current_results is None:
            return
            
        # Update summary
        self.update_summary()
        
        # Update performance chart
        self.update_performance_chart()
        
        # Update trades table
        self.update_trades_table()
        
        # Update positions table
        self.update_positions_table()
        
    def update_summary(self):
        """Update the summary text"""
        if self.current_results is None:
            return
            
        stat = self.current_results
        
        # Calculate conditional values
        win_rate = stat.wins/(stat.wins+stat.loses)*100 if stat.wins+stat.loses > 0 else 0
        avg_holding_days = stat.holdingDays/stat.sold if stat.sold > 0 else 0
        avg_win = stat.winAmt/stat.wins if stat.wins > 0 else 0
        avg_loss = stat.lossAmt/stat.loses if stat.loses > 0 else 0
        
        summary = f"""
BACKTEST SUMMARY
================

Strategy: {self.strategy_var.get()}
Period: {self.start_date_var.get()} to {self.end_date_var.get()}
Initial Cash: ${self.g_startCash:,.2f}

PERFORMANCE METRICS
------------------
Total Return: ${stat.profit:,.2f}
Return Rate: {(stat.cash+stat.stockVal-self.g_startCash)*100/(self.g_startCash-stat.minCash):.2f}%
Real Return: {(stat.cash+stat.stockVal-self.g_startCash)/self.g_startCash*100:.2f}%

TRADING STATISTICS
------------------
Total Trades: {stat.added}
Closed Trades: {stat.sold}
Wins: {stat.wins}
Losses: {stat.loses}
Win Rate: {win_rate:.1f}%

PORTFOLIO METRICS
-----------------
Max Positions: {stat.maxPositions}
Current Cash: ${stat.cash:,.2f}
Current Stock Value: ${stat.stockVal:,.2f}
Total Value: ${stat.cash+stat.stockVal:,.2f}
Min Cash: ${stat.minCash:,.2f}

RISK METRICS
------------
Max Value: ${stat.maxVal:,.2f}
Min Value: ${stat.minVal:,.2f}
Max Drawdown: {stat.maxDrawndown*100:.2f}%
Real Max Drawdown: {stat.maxRealDrawndown*100:.2f}%

AVERAGE METRICS
---------------
Avg Holding Days: {avg_holding_days:.1f}
Avg Win: ${avg_win:.2f}
Avg Loss: ${avg_loss:.2f}
        """
        
        self.summary_text.delete(1.0, tk.END)
        self.summary_text.insert(1.0, summary)
        
    def update_performance_chart(self):
        """Update the performance chart"""
        if self.current_results is None or not hasattr(self.current_results, 'dailyPositions'):
            return
            
        # Clear previous chart
        self.ax.clear()
        
        # Extract data for plotting
        dates = []
        portfolio_values = []
        cash_values = []
        stock_values = []
        
        for pos in self.current_results.dailyPositions:
            dates.append(pos['Date'])
            cash_values.append(pos['Cash'])
            stock_val = sum(holding.numShares * holding.price for holding in pos['Holdings'].values())
            stock_values.append(stock_val)
            portfolio_values.append(pos['Cash'] + stock_val)
        
        # Plot
        self.ax.plot(dates, portfolio_values, label='Total Portfolio', linewidth=2)
        self.ax.plot(dates, cash_values, label='Cash', alpha=0.7)
        self.ax.plot(dates, stock_values, label='Stock Value', alpha=0.7)
        
        self.ax.set_title('Portfolio Performance Over Time')
        self.ax.set_xlabel('Date')
        self.ax.set_ylabel('Value ($)')
        self.ax.legend()
        self.ax.grid(True, alpha=0.3)
        
        # Rotate x-axis labels
        plt.setp(self.ax.get_xticklabels(), rotation=45)
        
        self.canvas.draw()
        
    def update_trades_table(self):
        """Update the trades table"""
        # Clear existing items
        for item in self.trades_tree.get_children():
            self.trades_tree.delete(item)
            
        if self.current_results is None or not self.trade_history:
            return
            
        # Add trade data to the table
        for trade in self.trade_history:
            date_str = trade['date'].strftime("%Y-%m-%d")
            ticker = trade['ticker']
            action = trade['action']
            shares = trade['shares']
            price = f"${trade['price']:.2f}"
            profit = f"${trade['profit']:.2f}" if trade['profit'] != 0 else "N/A"
            
            # Color code the profit (green for positive, red for negative)
            if trade['profit'] > 0:
                profit = f"+${trade['profit']:.2f}"
            elif trade['profit'] < 0:
                profit = f"-${abs(trade['profit']):.2f}"
            
            self.trades_tree.insert("", "end", values=(date_str, ticker, action, shares, price, profit))
        
    def update_positions_table(self):
        """Update the positions table with current holdings"""
        # Clear existing items
        for item in self.positions_tree.get_children():
            self.positions_tree.delete(item)
            
        if self.current_results is None or not hasattr(self.current_results, 'dailyPositions') or not self.current_results.dailyPositions:
            return
            
        # Get the last day's positions
        last_position = self.current_results.dailyPositions[-1]
        holdings = last_position['Holdings']
        last_date = last_position['Date']
        
        # Add position data to the table
        for ticker, holding in holdings.items():
            shares = holding.numShares
            avg_price = holding.entryPrice
            current_price = holding.price
            holding_days = (last_date - holding.entryDate).days
            
            # Calculate open P/L
            open_pl = (current_price - avg_price) * shares
            pl_pct = ((current_price - avg_price) / avg_price) * 100 if avg_price > 0 else 0
            
            # Format values
            shares_str = f"{shares:,}"
            avg_price_str = f"${avg_price:.2f}"
            current_price_str = f"${current_price:.2f}"
            holding_days_str = f"{holding_days}"
            
            # Format P/L with color coding
            if open_pl > 0:
                open_pl_str = f"+${open_pl:,.2f}"
                pl_pct_str = f"+{pl_pct:.2f}%"
            elif open_pl < 0:
                open_pl_str = f"-${abs(open_pl):,.2f}"
                pl_pct_str = f"-{abs(pl_pct):.2f}%"
            else:
                open_pl_str = f"${open_pl:,.2f}"
                pl_pct_str = f"{pl_pct:.2f}%"
            
            self.positions_tree.insert("", "end", values=(
                ticker, shares_str, avg_price_str, current_price_str, 
                holding_days_str, open_pl_str, pl_pct_str
            ))
        
    def compare_strategies(self):
        """Compare multiple strategies"""
        if self.data_by_date is None:
            messagebox.showerror("Error", "Please load data first!")
            return
            
        # This would implement strategy comparison
        # For now, show a message
        messagebox.showinfo("Info", "Strategy comparison feature coming soon!")
        
    def export_results(self):
        """Export results to file"""
        if self.current_results is None:
            messagebox.showerror("Error", "No results to export!")
            return
            
        filename = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        
        if filename:
            try:
                # Export results
                export_data = {
                    "strategy": self.strategy_var.get(),
                    "start_date": self.start_date_var.get(),
                    "end_date": self.end_date_var.get(),
                    "initial_cash": self.g_startCash,
                    "final_cash": self.current_results.cash,
                    "total_profit": self.current_results.profit,
                    "return_rate": (self.current_results.cash+self.current_results.stockVal-self.g_startCash)*100/(self.g_startCash-self.current_results.minCash),
                    "max_positions": self.current_results.maxPositions,
                    "total_trades": self.current_results.added,
                    "closed_trades": self.current_results.sold,
                    "wins": self.current_results.wins,
                    "losses": self.current_results.loses
                }
                
                with open(filename, 'w') as f:
                    json.dump(export_data, f, indent=2)
                    
                messagebox.showinfo("Success", f"Results exported to {filename}")
                
            except Exception as e:
                messagebox.showerror("Error", f"Failed to export results: {str(e)}")


def main():
    root = tk.Tk()
    app = TradingBacktestGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main() 