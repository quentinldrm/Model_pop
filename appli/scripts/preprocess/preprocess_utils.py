import yaml

def load_config(path="appli/config/settings.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def print_status(step: str, status: str = "ok", detail: str = ""):
    prefix = {"ok": "[✓]", "err": "[✗]", "info": "[→]"}
    symbol = prefix.get(status, "[...]")
    message = f"{symbol} {step}"
    if detail:
        message += f" : {detail}"
    print(message)
