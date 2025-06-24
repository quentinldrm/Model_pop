import os
import subprocess
import sys
import platform

def create_venv():
    print("Création de l'environnement virtuel (.venv)...")
    subprocess.run([sys.executable, "-m", "venv", ".venv"])

def activate_venv_instruction():
    if platform.system() == "Windows":
        return ".venv\\Scripts\\activate"
    else:
        return "source .venv/bin/activate"

def install_requirements():
    print("Installation des dépendances...")
    pip_exec = ".venv\\Scripts\\pip.exe" if platform.system() == "Windows" else ".venv/bin/pip"
    subprocess.run([pip_exec, "install", "-r", "requirements.txt"])

if __name__ == "__main__":
    create_venv()
    print(f"Environnement virtuel créé. Active-le avec :\n\n    {activate_venv_instruction()}\n")
    install_requirements()
    print("Tout est prêt. Consultez le fichier README.md pour les instrctions suivantes.")

