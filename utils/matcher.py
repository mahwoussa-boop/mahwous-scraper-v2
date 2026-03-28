import pandas as pd
import re
from thefuzz import fuzz
import logging

logger = logging.getLogger(__name__)

class SmartMatcher:
    def __init__(self, fuzzy_threshold=88):
        self.fuzzy_threshold = fuzzy_threshold
        # Regex to capture English and Arabic volume variations
        self.vol_pattern = re.compile(r'(\d+(?:\.\d+)?)\s*(ml|oz|مل|لتر)', re.IGNORECASE)

    def _extract_volume(self, text):
        """Extracts and normalizes volume to ML for safe comparison."""
        if not isinstance(text, str):
            return None
        match = self.vol_pattern.search(text)
        if match:
            try:
                val = float(match.group(1))
                unit = match.group(2).lower()
                if unit == 'oz':
                    val = val * 29.5735  # Convert oz to ml
                elif unit == 'لتر':
                    val = val * 1000  # Convert liter to ml
                return round(val, 1)
            except ValueError:
                return None
        return None

    def match_products(self, df_mine: pd.DataFrame, df_comp: pd.DataFrame) -> pd.DataFrame:
        logger.info("Starting Hybrid Smart Matching Process...")
        df_m = df_mine.copy()
        df_c = df_comp.copy()
        results = []

        # Ensure text columns exist
        if 'name' not in df_m.columns:
            df_m['name'] = ''
        if 'name' not in df_c.columns:
            df_c['name'] = ''
        if 'sku' not in df_m.columns:
            df_m['sku'] = ''
        if 'sku' not in df_c.columns:
            df_c['sku'] = ''
        df_m['sku'] = df_m['sku'].fillna('').astype(str)
        df_c['sku'] = df_c['sku'].fillna('').astype(str)

        # STEP A: Exact Match via Barcode/EAN (if columns exist)
        if 'ean' in df_m.columns and 'ean' in df_c.columns:
            # Filter rows with valid EANs
            valid_m = df_m[df_m['ean'].notna() & (df_m['ean'] != '')]
            valid_c = df_c[df_c['ean'].notna() & (df_c['ean'] != '')]

            exact_matches = pd.merge(valid_m, valid_c, on='ean', suffixes=('_mine', '_comp'))

            for _, r in exact_matches.iterrows():
                results.append({
                    'sku_mine': r.get('sku_mine', ''),
                    'name_mine': r.get('name_mine', ''),
                    'sku_comp': r.get('sku_comp', ''),
                    'name_comp': r.get('name_comp', ''),
                    'match_type': 'Exact EAN',
                    'match_score': 100.0
                })

            # Remove exact matches from the pool
            matched_m_skus = [x['sku_mine'] for x in results]
            matched_c_skus = [x['sku_comp'] for x in results]
            if 'sku' in df_m.columns:
                df_m = df_m[~df_m['sku'].isin(matched_m_skus)]
            if 'sku' in df_c.columns:
                df_c = df_c[~df_c['sku'].isin(matched_c_skus)]

        # STEP B: Strict Fuzzy Match with Volume Gatekeeper
        df_m['extracted_vol'] = df_m['name'].apply(self._extract_volume)
        df_c['extracted_vol'] = df_c['name'].apply(self._extract_volume)

        for _, row_m in df_m.iterrows():
            best_match = None
            best_score = 0

            for _, row_c in df_c.iterrows():
                # STRICT RULE: VOLUME GATE
                vol_m = row_m['extracted_vol']
                vol_c = row_c['extracted_vol']

                # If both have a detected volume, they MUST match within 0.5ml tolerance
                if pd.notna(vol_m) and pd.notna(vol_c):
                    if abs(vol_m - vol_c) > 0.6:
                        continue  # Skip to next competitor product immediately

                # Fuzzy score calculation
                score = fuzz.token_sort_ratio(str(row_m['name']), str(row_c['name']))

                if score >= self.fuzzy_threshold and score > best_score:
                    best_score = score
                    best_match = row_c

            if best_match is not None:
                results.append({
                    'sku_mine': row_m.get('sku', ''),
                    'name_mine': row_m.get('name', ''),
                    'sku_comp': best_match.get('sku', ''),
                    'name_comp': best_match.get('name', ''),
                    'match_type': 'Fuzzy + Volume Gate',
                    'match_score': float(best_score)
                })
                # Remove matched competitor product to avoid duplicates
                if 'sku' in df_c.columns:
                    df_c = df_c[df_c['sku'].astype(str) != str(best_match.get('sku', ''))]
                else:
                    df_c = df_c.drop(index=best_match.name)

        logger.info(f"Matching complete. Found {len(results)} total matches.")
        return pd.DataFrame(results)
