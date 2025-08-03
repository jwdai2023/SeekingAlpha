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
    Status, optimize_data_loading, run_optimized_backtest, print_results
)


class TradingBacktestGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Alpha Trading Backtest GUI")
        self.root.geometry("1400x900")
        
        # Data storage
        self.data_by_date = None
        self.current_results = None
        self.strategy_instances = {}
        
        # Create main frames
        self.create_widgets()
        
        # Initialize strategy instances
        self.initialize_strategies()
        
    def create_widgets(self):
        # Main container
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Left panel - Controls
        left_panel = ttk.Frame(main_frame, width=400)
        left_panel.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10))
        
        # Right panel - Results and Charts
        right_panel = ttk.Frame(main_frame)
        right_panel.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        
        self.create_control_panel(left_panel)
        self.create_results_panel(right_panel)
        
    def create_control_panel(self, parent):
        # Title
        title_label = ttk.Label(parent, text="Trading Backtest Controls", font=("Arial", 14, "bold"))
        title_label.pack(pady=(0, 20))
        
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
        
        # Initial cash
        ttk.Label(portfolio_frame, text="Initial Cash:").pack(anchor=tk.W)
        self.initial_cash_var = tk.StringVar(value="350000")
        ttk.Entry(portfolio_frame, textvariable=self.initial_cash_var).pack(fill=tk.X, pady=(0, 10))
        
        # Trade size
        ttk.Label(portfolio_frame, text="Trade Size:").pack(anchor=tk.W)
        self.trade_size_var = tk.StringVar(value="6000")
        ttk.Entry(portfolio_frame, textvariable=self.trade_size_var).pack(fill=tk.X, pady=(0, 10))
        
        # Max positions
        ttk.Label(portfolio_frame, text="Max Positions:").pack(anchor=tk.W)
        self.max_positions_var = tk.StringVar(value="2000")
        ttk.Entry(portfolio_frame, textvariable=self.max_positions_var).pack(fill=tk.X, pady=(0, 10))
        
        # Min cash
        ttk.Label(portfolio_frame, text="Min Cash:").pack(anchor=tk.W)
        self.min_cash_var = tk.StringVar(value="20000")
        ttk.Entry(portfolio_frame, textvariable=self.min_cash_var).pack(fill=tk.X, pady=(0, 10))
        
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
        
        trades_scrollbar = ttk.Scrollbar(self.trades_frame, orient=tk.VERTICAL, command=self.trades_tree.yview)
        self.trades_tree.configure(yscrollcommand=trades_scrollbar.set)
        
        self.trades_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        trades_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
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
        
        # Create parameter widgets
        self.param_vars = {}
        for param_name, display_name, default_value in params:
            ttk.Label(self.params_frame, text=f"{display_name}:").pack(anchor=tk.W)
            var = tk.StringVar(value=str(default_value))
            self.param_vars[param_name] = var
            ttk.Entry(self.params_frame, textvariable=var).pack(fill=tk.X, pady=(0, 5))
            
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
                
                # Load data using optimized function
                self.data_by_date = optimize_data_loading()
                
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
                global g_startDate, g_endDate, g_startCash, g_tradeSize, g_maxPostion, g_minCash
                g_startDate = datetime.strptime(self.start_date_var.get(), "%Y-%m-%d").date()
                g_endDate = datetime.strptime(self.end_date_var.get(), "%Y-%m-%d").date()
                g_startCash = float(self.initial_cash_var.get())
                g_tradeSize = float(self.trade_size_var.get())
                g_maxPostion = int(self.max_positions_var.get())
                g_minCash = float(self.min_cash_var.get())
                
                # Run backtest
                self.current_results = self.run_custom_backtest()
                
                # Update GUI
                self.root.after(0, self.update_results_display)
                
                self.progress_var.set("Backtest completed!")
                self.run_btn.config(state="normal")
                
            except Exception as e:
                self.progress_var.set("Error running backtest")
                self.run_btn.config(state="normal")
                self.root.after(0, lambda: messagebox.showerror("Error", f"Backtest failed: {str(e)}"))
        
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
                    
    def run_custom_backtest(self):
        """Run backtest with custom parameters"""
        # This is a simplified version of the optimized backtest
        # You can copy the full implementation from alphaTrade_optimized.py
        
        stat = Status(g_startCash)
        strategy = self.strategy_instances[self.strategy_var.get()]
        holdings = {}
        
        # Pre-calculate trading dates
        trading_dates = []
        for single_date in self.daterange(g_startDate, g_endDate):
            if single_date.weekday() < 5:
                trading_dates.append(single_date)
        
        # Run backtest logic here...
        # (This is a placeholder - you'll need to implement the full backtest logic)
        
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
Initial Cash: ${g_startCash:,.2f}

PERFORMANCE METRICS
------------------
Total Return: ${stat.profit:,.2f}
Return Rate: {(stat.cash+stat.stockVal-g_startCash)*100/(g_startCash-stat.minCash):.2f}%
Real Return: {(stat.cash+stat.stockVal-g_startCash)/g_startCash*100:.2f}%

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
            stock_values.append(pos['Cash'] + pos.get('stockVal', 0))
            portfolio_values.append(pos['Cash'] + pos.get('stockVal', 0))
        
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
            
        if self.current_results is None:
            return
            
        # Add trade data (this would need to be implemented based on your trade tracking)
        # For now, this is a placeholder
        
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
                    "initial_cash": g_startCash,
                    "final_cash": self.current_results.cash,
                    "total_profit": self.current_results.profit,
                    "return_rate": (self.current_results.cash+self.current_results.stockVal-g_startCash)*100/(g_startCash-self.current_results.minCash),
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