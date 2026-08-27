"""alignment_ob — iterate the TORA live feed toward research-aligned signals.

Stages (docs/tora_onramp.md): M0 raw capture → M1 normalize → M2 converters
(live L2 → place/trade + tick_table) → M3 alignment profiling vs the research
tape → M4 live cqr_bar. Everything that does not need the TORA SDK is built
and tested offline here against the real research data.
"""
