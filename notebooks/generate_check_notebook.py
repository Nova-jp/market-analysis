import json

notebook_content = {
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# 市中残存額 (Market Amount) 確認用ノートブック\n",
    "\n",
    "以下の変数 `target_bonds` に指定した3つの銘柄について、市中残存額の推移をグラフ表示します。"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "import sys\n",
    "import os\n",
    "import pandas as pd\n",
    "import plotly.express as px\n",
    "import plotly.graph_objects as go\n",
    "\n",
    "# Add project root to path if running from notebooks directory\n",
    "if os.getcwd().endswith('notebooks'):\n",
    "    sys.path.append(os.path.abspath('..'))\n",
    "else:\n",
    "    sys.path.append(os.path.abspath('.'))\n",
    "\n",
    "from data.utils.database_manager import DatabaseManager\n",
    "\n",
    "# Initialize Database Manager\n",
    "db = DatabaseManager()\n",
    "print(\"Database connected.\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "def get_bond_market_amount(bond_code):\n",
    "    query = (\n",
    "        \"SELECT trade_date, market_amount \"\n",
    "        \"FROM bond_market_amount \"\n",
    "        \"WHERE bond_code = %s \"\n",
    "        \"ORDER BY trade_date\"\n",
    "    )\n",
    "    rows = db.select_as_dict(query, (bond_code,))\n",
    "    if not rows:\n",
    "        print(f\"No data found for {bond_code}\")\n",
    "        return pd.DataFrame()\n",
    "    df = pd.DataFrame(rows)\n",
    "    df['trade_date'] = pd.to_datetime(df['trade_date'])\n",
    "    df['market_amount'] = df['market_amount'].astype(float)\n",
    "    return df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# === 銘柄指定エリア ===\n",
    "# ここに確認したい銘柄コードを3つ指定してください\n",
    "target_bonds = [\n",
    "    '002700074',  # サンプル銘柄1\n",
    "    '003300074',  # サンプル銘柄2\n",
    "    '002680074'   # サンプル銘柄3\n",
    "]\n",
    "\n",
    "print(f\"Selected bonds: {target_bonds}\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "fig = go.Figure()\n",
    "\n",
    "for b_code in target_bonds:\n",
    "    if not b_code:\n",
    "        continue\n",
    "        \n",
    "    df = get_bond_market_amount(b_code)\n",
    "    if df.empty:\n",
    "        continue\n",
    "        \n",
    "    fig.add_trace(go.Scatter(\n",
    "        x=df['trade_date'],\n",
    "        y=df['market_amount'],\n",
    "        mode='lines',\n",
    "        name=f'Bond {b_code}'\n",
    "    ))\n",
    "\n",
    "fig.update_layout(\n",
    "    title='市中残存額 (Market Amount) 推移',\n",
    "    xaxis_title='日付',\n",
    "    yaxis_title='市中残存額 (億円)',\n",
    "    hovermode='x unified',\n",
    "    template='plotly_white',\n",
    "    height=600\n",
    ")\n",
    "fig.show()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.10"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}

with open('notebooks/market_amount_check.ipynb', 'w') as f:
    json.dump(notebook_content, f, indent=2)

print("Notebook created: notebooks/market_amount_check.ipynb")
