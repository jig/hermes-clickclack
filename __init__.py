def register(ctx):
    """Plugin entry point; the adapter module is imported lazily."""
    from .adapter import register as _register

    _register(ctx)


__all__ = ["register"]
