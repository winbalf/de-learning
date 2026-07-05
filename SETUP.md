# Project Setup

Quick reference for activating the virtual environment and running scripts in this repo.

**Environment:** WSL2 Ubuntu · Python 3.11 · PostgreSQL 15 (Docker)

---

## Quick start (returning to the project)

Every time you open a new terminal, activate the virtual environment before running Python scripts:

```bash
cd ~/de-learning
source .venv/bin/activate
```

Your prompt should show `(.venv)` at the start. Then run scripts as usual:

```bash
cd week1
python explore.py

cd ../week2
python window_functions.py
python query_optimization.py
```

**Without activating**, you can call the venv Python directly:

```bash
~/de-learning/.venv/bin/python week2/query_optimization.py
```

To leave the virtual environment:

```bash
deactivate
```

---

## First-time setup

Follow these steps once when setting up a new machine.

### 1. System packages (pyenv dependencies)

```bash
sudo apt update && sudo apt upgrade -y

sudo apt install -y make build-essential libssl-dev zlib1g-dev \
  libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
  libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev \
  libffi-dev liblzma-dev git
```

### 2. Install pyenv

```bash
curl https://pyenv.run | bash

echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc
source ~/.bashrc

pyenv --version
```

### 3. Install Python 3.11

```bash
pyenv install 3.11.9
pyenv global 3.11.9
python --version   # Python 3.11.9
pip install --upgrade pip
```

### 4. Clone or create the project folder

```bash
mkdir -p ~/de-learning
cd ~/de-learning
```

If you already have the repo, just `cd` into it.

### 5. Create and activate the virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 6. Install Python dependencies

```bash
pip install pandas polars pyarrow jupyter ipykernel sqlalchemy psycopg2-binary
```

Verify:

```bash
python -c "import pandas, sqlalchemy; print('OK')"
```

### 7. Connect Cursor / VS Code to WSL

1. Install the **WSL** extension (Windows side).
2. Open the project from a WSL terminal:

```bash
cd ~/de-learning
code .
```

3. Install the **Python** extension when prompted.
4. Select the project interpreter: `Ctrl+Shift+P` → **Python: Select Interpreter** → choose `./.venv/bin/python`.

Scripts run from the integrated terminal will use the venv once the interpreter is selected.

---

## PostgreSQL (Docker)

Week 1 and Week 2 scripts connect to a local Postgres container.

### Start the database (first time)

```bash
sudo apt install -y docker.io
sudo service docker start
sudo usermod -aG docker $USER
newgrp docker

docker run --name de-postgres \
  -e POSTGRES_USER=deuser \
  -e POSTGRES_PASSWORD=depassword \
  -e POSTGRES_DB=delearning \
  -p 5433:5432 \
  -d postgres:15

docker ps   # confirm de-postgres is running
```

### Connection details

| Setting  | Value |
| -------- | ----- |
| Host     | `localhost` |
| Port     | `5433` |
| Database | `delearning` |
| User     | `deuser` |
| Password | `depassword` |

Connection string used in scripts:

```
postgresql+psycopg2://deuser:depassword@localhost:5433/delearning
```

### Start / stop later

```bash
docker start de-postgres    # start existing container
docker stop de-postgres     # stop
docker ps                   # check status
```

### Load the titanic table

The `titanic` table is created by the Week 1 pipeline. Run this before Week 2 SQL scripts:

```bash
cd ~/de-learning
source .venv/bin/activate
cd week1
python explore.py          # creates titanic_cleaned.parquet
python db_pipeline.py      # loads data into PostgreSQL
```

---

## Project layout

```
de-learning/
├── .venv/                 # virtual environment (not committed)
├── SETUP.md               # this file
├── README.md              # Week 1 study plan
├── week1/
│   ├── explore.py
│   ├── db_pipeline.py
│   ├── parquet_vs_csv.py
│   └── run_pipeline.sh
└── week2/
    ├── window_functions.py
    └── query_optimization.py
```

---

## Troubleshooting

### `ModuleNotFoundError: No module named 'pandas'`

You are using system Python instead of the project venv. Fix:

```bash
cd ~/de-learning
source .venv/bin/activate
python -c "import pandas; print(pandas.__version__)"
```

Or use the full path: `~/de-learning/.venv/bin/python script.py`

### `connection refused` on port 5433

Postgres is not running:

```bash
docker start de-postgres
docker ps
```

If the container does not exist, recreate it with the `docker run` command above.

### `column "name" does not exist` (PostgreSQL)

The `titanic` table uses PascalCase column names from pandas (`"Name"`, `"Fare"`, `"Pclass"`, etc.). Quote them in SQL:

```sql
SELECT "Name", "Fare" FROM titanic WHERE "Fare" > 100;
```

### Virtual environment missing

Recreate it:

```bash
cd ~/de-learning
python -m venv .venv
source .venv/bin/activate
pip install pandas polars pyarrow jupyter ipykernel sqlalchemy psycopg2-binary
```

---

## Daily workflow checklist

1. Open WSL terminal (or Cursor integrated terminal).
2. `cd ~/de-learning && source .venv/bin/activate`
3. `docker start de-postgres` (if not already running)
4. Run your script from the correct week folder.
