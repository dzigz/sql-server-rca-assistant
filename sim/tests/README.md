# Tests

Stage-1 tests focus on:
- web session management/persistence
- SQL Server health tools behavior
- core config/container utilities still in use

## Run

```bash
source sim/.venv/bin/activate
pytest sim/tests -v
```

## Recommended Fast Subset

```bash
pytest sim/tests/test_webapp sim/tests/test_rca/test_health_tools.py -v
```

## Notes

Simulation/workflow/critique test suites were removed with the stage-1 scope cut.
