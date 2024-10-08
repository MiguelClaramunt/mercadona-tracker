# %%
from pathlib import Path
current_dir = Path(__file__)
project_dir = [p for p in Path(__file__).parents if p.parts[-1]=='mercadona-tracker'][0]
# %%
project_dir