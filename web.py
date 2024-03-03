from flask import Flask, render_template, request
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
from matplotlib.dates import YearLocator

app = Flask(__name__, template_folder='html')

# Load CSV data into DataFrames (assuming 'sma_50.csv' and 'sma_200.csv' exist)
sma_50_df = pd.read_csv("csv/sma_50/sma_50.csv")
sma_200_df = pd.read_csv("csv/sma_200/sma_200.csv")

@app.route("/")
def home():
    # Get unique symbols from both DataFrames
    symbols = list(set(sma_50_df["symbol"].unique().tolist() + sma_200_df["symbol"].unique().tolist()))
    return render_template("index.html", symbols=symbols)

@app.route("/plot", methods=["POST"])
def plot():
    # Get selected symbol from form
    selected_symbol = request.form.get("symbol")

    # Filter data for the selected symbol from both DataFrames
    sma_50_data = sma_50_df[sma_50_df["symbol"] == selected_symbol]
    sma_200_data = sma_200_df[sma_200_df["symbol"] == selected_symbol]

    # Check if data exists for the selected symbol
    if not sma_50_data.empty and not sma_200_data.empty:
        # Create the plot
        plt.figure(figsize=(12, 10))
        plt.plot(sma_50_data["date"], sma_50_data["moving_avg_50"], label="SMA 50")
        plt.plot(sma_200_data["date"], sma_200_data["moving_avg_200"], label="SMA 200")
        plt.xlabel("Date")
        plt.ylabel("Moving Average")
        plt.title(f"SMA Comparison for {selected_symbol}")
        plt.legend()

        plt.xticks(rotation=45)
        plt.gca().xaxis.set_major_locator(YearLocator())
        # Convert plot to image data
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format="png")
        img_data = base64.b64encode(img_buffer.getvalue()).decode("utf-8")

        return render_template("plot.html", image_data=img_data, symbol=selected_symbol)
    else:
        return "No data found for the selected symbol."

if __name__ == "__main__":
    app.run(debug=True)
