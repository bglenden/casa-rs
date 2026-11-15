This CASA 6.7.6.14 voltage-pattern table preserves the T1 EVLA fixtures' defined
input beam: an ideal unblocked 25 m Airy aperture. The native Simulator's default
EVLA beam remains CASA's frequency-dependent fitted beam.

Generated with `casatools.vpmanager`:

```python
vp = vpmanager()
vp.reset()
vp.setpbairy(telescope="EVLA", dishdiam="25m", blockagediam="0m",
             maxrad="0.8564deg", reffreq="1GHz", dosquint=False)
vp.saveastable("evla-ideal-25m.vp")
```
