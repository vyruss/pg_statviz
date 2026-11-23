import importlib

analyze_module = importlib.import_module('pg_statviz.modules.analyze')


def test_analyze_gives_every_module_the_port(monkeypatch):
    # The modules analyze runs, as imported into it
    modules = [name for name, f in vars(analyze_module).items()
               if name != 'analyze' and getattr(f, '__module__', '')
               .startswith('pg_statviz.modules.')]
    ports = {}
    for name in modules:
        monkeypatch.setattr(analyze_module, name,
                            lambda name=name, **kwargs:
                            ports.update({name: kwargs.get('port')}))
    monkeypatch.setattr(analyze_module, 'dbconn', lambda **kwargs: None)
    monkeypatch.setattr(analyze_module, 'getinfo',
                        lambda conn: {'hostname': 'testhost'})
    analyze_module.analyze(port='5433')
    assert ports == dict.fromkeys(modules, '5433')
