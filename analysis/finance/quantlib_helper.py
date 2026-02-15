import QuantLib as ql
import pandas as pd
from datetime import datetime
from typing import List, Optional, Dict, Any

class QuantLibHelper:
    def __init__(self, date_str: str):
        """
        Initialize QuantLib Helper for a specific evaluation date.
        """
        self.date_str = date_str
        self.eval_date = self._parse_date(date_str)
        
        # Set evaluation date globally for QuantLib
        ql.Settings.instance().evaluationDate = self.eval_date
        
        # Japan Market Conventions
        self.calendar = ql.Japan()
        self.currency = ql.JPYCurrency()
        self.day_counter = ql.Actual365Fixed() # Standard for OIS Curve Construction?
        # TONA OIS usually uses Act/365
        
        # TONA Index
        self.tona_index = ql.OvernightIndex("TONA", 0, ql.JPYCurrency(), ql.Japan(), ql.Actual365Fixed())
        
        self.yield_curve_handle = None
        
        # Cache for MMS calculation results to avoid redundant computations
        # Key: (maturity_date_str, fixed_freq_str, fixed_day_count_str, float_spread)
        self._mms_cache = {}
        self._forward_cache = {}

    def _parse_date(self, date_str: str) -> ql.Date:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        return ql.Date(d.day, d.month, d.year)

    def _parse_tenor(self, tenor_str: str) -> ql.Period:
        if tenor_str.endswith('D'):
            return ql.Period(int(tenor_str[:-1]), ql.Days)
        elif tenor_str.endswith('W'):
            return ql.Period(int(tenor_str[:-1]), ql.Weeks)
        elif tenor_str.endswith('M'):
            return ql.Period(int(tenor_str[:-1]), ql.Months)
        elif tenor_str.endswith('Y'):
            return ql.Period(int(tenor_str[:-1]), ql.Years)
        else:
            raise ValueError(f"Unknown tenor format: {tenor_str}")

    def build_ois_curve(self, ois_data: list):
        """
        Builds the OIS Yield Curve.
        ois_data: list of dicts {'tenor': str, 'rate': float} (rate in %)
        """
        rate_helpers = []
        
        # Sort data by tenor length to ensure correct order
        # Need a custom sorter... or just rely on the helpers handling it (they usually do if unique)
        # But good to feed in order.
        
        for item in ois_data:
            tenor = self._parse_tenor(item['tenor'])
            rate = item['rate'] / 100.0 # Convert % to decimal
            
            quote = ql.QuoteHandle(ql.SimpleQuote(rate))
            
            # TONA OIS Helper
            # Settlement days: 0 (start today) or 2 (spot)?
            # TONA OIS is usually T+2 settlement for longer tenors?
            # Actually, standard OIS helper assumes settlement days compatible with the index.
            # Let's assume 2 for now as it's standard for swaps, but OIS might be 0.
            # Checking market convention: JPY OIS is usually T+0 (Trade Date) start for very short, but T+2 for standard curve?
            # Let's use 2 settlement days which is standard for most OIS helpers in examples unless specified.
            # Wait, `OISRateHelper` takes settlementDays.
            
            helper = ql.OISRateHelper(
                2,                  # settlement days
                tenor,              # tenor
                quote,              # rate
                self.tona_index     # index
            )
            rate_helpers.append(helper)
            
        # Build the curve
        self.yield_curve = ql.PiecewiseLogCubicDiscount(
            2, # settlement days (reference date = eval date + 2)
            self.calendar,
            rate_helpers,
            self.day_counter
        )
        self.yield_curve.enableExtrapolation()
        self.yield_curve_handle = ql.YieldTermStructureHandle(self.yield_curve)
        
        # Link the index to this curve using clone
        self.tona_index = self.tona_index.clone(self.yield_curve_handle)

    def calculate_mms(self, maturity_date_str: str, 
                      fixed_freq_str: str = 'Annual', 
                      fixed_day_count_str: str = 'Act365',
                      float_spread: float = 0.0) -> float:
        """
        Calculates the Matched Maturity Swap (MMS) rate.
        
        Args:
            maturity_date_str: Maturity date of the bond (YYYY-MM-DD)
            fixed_freq_str: Frequency of the Fixed Leg (Bond side). 'Annual' or 'Semiannual'.
            fixed_day_count_str: Day Count Convention of the Fixed Leg. 'Act365' or '30/360'.
            float_spread: Spread added to the Floating Leg (TONA). Default 0.0.
            
        Returns:
            Par Swap Rate (Fixed Rate) in % that makes NPV of swap = 0.
        """
        # Check cache first
        cache_key = (maturity_date_str, fixed_freq_str, fixed_day_count_str, float_spread)
        if cache_key in self._mms_cache:
            return self._mms_cache[cache_key]

        if not self.yield_curve_handle:
            raise ValueError("Yield Curve not built. Call build_ois_curve first.")
            
        maturity_date = self._parse_date(maturity_date_str)
        
        # Spot Date (Start Date) -> T+2 is standard for JPY Swaps (even if OIS index is T+0)
        # However, for ASW matching a specific bond, we align with the settlement of the bond trade?
        # Usually standard spot lag is used.
        spot_date = self.calendar.advance(self.eval_date, 2, ql.Days)
        
        if maturity_date <= spot_date:
            return 0.0 

        # --- 1. Fixed Leg Configuration (mimicking the Bond) ---
        
        # Frequency
        if fixed_freq_str.lower() == 'annual':
            fixed_freq = ql.Annual
        elif fixed_freq_str.lower() == 'semiannual':
            fixed_freq = ql.Semiannual
        else:
            raise ValueError(f"Unsupported fixed frequency: {fixed_freq_str}")
            
        # Day Count
        if fixed_day_count_str.lower() == 'act365':
            fixed_dc = ql.Actual365Fixed()
        elif fixed_day_count_str.lower() == '30/360':
            fixed_dc = ql.Thirty360(ql.Thirty360.BondBasis)
        else:
            raise ValueError(f"Unsupported fixed day count: {fixed_day_count_str}")

        # Schedule
        # Adjusted maturity for holidays
        maturity_date_adj = self.calendar.adjust(maturity_date)

        fixed_schedule = ql.Schedule(
            spot_date,
            maturity_date_adj,
            ql.Period(fixed_freq),
            self.calendar,
            ql.ModifiedFollowing,
            ql.ModifiedFollowing,
            ql.DateGeneration.Backward,
            False
        )
        
        # --- 2. Floating Leg Configuration (Market Standard for OIS Swap) ---
        # TONA OIS usually pays Annually (Act/365).
        # However, for Asset Swaps, the floating leg might match the fixed leg frequency or use standard money market conventions.
        # Standard Vanilla Overnight Index Swap:
        #   - Fixed: Annual, Act/365
        #   - Float: Daily compounding, Annual payment, Act/365
        
        # In ASW, we solve for the Fixed Rate that matches the Floating Leg (TONA + 0).
        # We use the OvernightIndexedSwap constructor which defaults the float leg schedule to match the fixed leg?
        # No, `OvernightIndexedSwap` constructor takes a schedule and uses it for BOTH legs if not specified otherwise, 
        # OR it assumes the float payments match the fixed schedule.
        # In `ql.OvernightIndexedSwap(type, nominal, schedule, fixedRate, fixedDC, overnightIndex, spread)`:
        # The `schedule` applies to the FIXED leg.
        # The floating leg payments also follow this schedule (i.e. if Fixed is SemiAnnual, Float pays SemiAnnual).
        # This is typically how ASW is structured (legs match dates).
        
        # Note on Floating Day Count: The Index (self.tona_index) carries its own day count (Act/365 for TONA).
        # The swap engine uses the index's day count for the floating leg calculation.
        
        dummy_rate = 0.01
        nominal = 10000.0
        
        try:
            swap = ql.OvernightIndexedSwap(
                ql.OvernightIndexedSwap.Payer, # Payer of Fixed (Bond logic: Bond pays fixed, we pay float? No, ASW is usually: Investor Long Bond + Pay Fixed / Rec Float? Or Pay Float / Rec Fixed?)
                # Actually for "MMS Rate", we just want the Par Rate. Direction doesn't matter for the rate itself.
                nominal,
                fixed_schedule,
                dummy_rate,
                fixed_dc,
                self.tona_index,
                float_spread
            )
            
            # Pricing Engine
            engine = ql.DiscountingSwapEngine(self.yield_curve_handle)
            swap.setPricingEngine(engine)
            
            fair_rate = swap.fairRate()
            result = fair_rate * 100.0 # Convert to %
            
            # Save to cache
            self._mms_cache[cache_key] = result
            return result
            
        except Exception as e:
            print(f"Error calculating MMS for {maturity_date_str}: {e}")
            # import traceback
            # traceback.print_exc()
            return None

    def calculate_forward_swap_rate(self, start_tenor_str: str, swap_tenor_str: str) -> Optional[float]:
        """
        Calculates the forward par swap rate.
        
        Args:
            start_tenor_str: Start of the swap (e.g., '1Y')
            swap_tenor_str: Tenor of the swap (e.g., '5Y')
            
        Returns:
            Forward Par Swap Rate in %
        """
        cache_key = (start_tenor_str, swap_tenor_str)
        if cache_key in self._forward_cache:
            return self._forward_cache[cache_key]

        if not self.yield_curve_handle:
            raise ValueError("Yield Curve not built. Call build_ois_curve first.")

        try:
            start_tenor = self._parse_tenor(start_tenor_str)
            swap_tenor = self._parse_tenor(swap_tenor_str)
            
            # Use standard OIS conventions: Annual/Act365
            # QuantLib has OvernightIndexedSwapIndex which simplifies forward rate calculation
            
            # The forward start date is usually (EvalDate + 2 days) + start_tenor
            # But standard forward swap definitions might vary.
            # Here we follow the convention of standard OIS:
            # Spot = Eval + 2
            # Forward Start = Spot + start_tenor
            # Maturity = Forward Start + swap_tenor
            
            spot_date = self.calendar.advance(self.eval_date, 2, ql.Days)
            forward_start_date = self.calendar.advance(spot_date, start_tenor)
            maturity_date = self.calendar.advance(forward_start_date, swap_tenor)
            
            # Define schedule for the forward swap
            schedule = ql.Schedule(
                forward_start_date,
                maturity_date,
                ql.Period(ql.Annual),
                self.calendar,
                ql.ModifiedFollowing,
                ql.ModifiedFollowing,
                ql.DateGeneration.Backward,
                False
            )
            
            # Create a dummy OvernightIndexedSwap to calculate the fair rate
            swap = ql.OvernightIndexedSwap(
                ql.OvernightIndexedSwap.Payer,
                10000.0,
                schedule,
                0.01, # dummy rate
                ql.Actual365Fixed(),
                self.tona_index
            )
            
            engine = ql.DiscountingSwapEngine(self.yield_curve_handle)
            swap.setPricingEngine(engine)
            
            fair_rate = swap.fairRate()
            result = fair_rate * 100.0
            
            self._forward_cache[cache_key] = result
            return result
            
        except Exception as e:
            print(f"Error calculating forward swap rate for {start_tenor_str}x{swap_tenor_str}: {e}")
            return None

