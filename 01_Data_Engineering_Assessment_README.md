# Data Engineer Assessment

In this assessment, you are required to work with the Malaysian fuel price data from [data.gov.my](https://data.gov.my). Specifically, you will be focusing on the data source:  
[https://api.data.gov.my/data-catalogue?id=fuelprice](https://api.data.gov.my/data-catalogue?id=fuelprice)

---

## 1. Data Extraction

1. **Script Requirements**  
   - Create a Python script (can be part of a notebook or a separate `.py` file) that uses the `requests` library to make an HTTP GET request to the API endpoint.  
   - Include at least one example of:
     - **Exception handling**: Wrap the request call in `try-except` blocks to catch any errors (e.g., connection errors or JSON decoding errors).  
     - **Retries**: If the initial request fails, implement a simple retry mechanism (e.g., retry 3 times with a short wait in between).
     - **Logging**: Use Python's built-in `logging` library (or a similar framework) to log the status of requests and any errors that occur.

2. **Data Check**  
   - Print out or display a sample of the JSON response to verify that you have correctly retrieved the data.  

---

## 2. Data Transformation

After you have successfully extracted the data, perform some data transformations. You have the freedom to approach these transformations in a way that makes sense to you, but here are a couple of ideas:

- **Example 1**:  
  - Convert the JSON response into a Pandas DataFrame.  
  - Rename columns for clarity (e.g., rename `ron95` to `RON95_price`, `diesel` to `Diesel_price`, etc.).  
  - Convert string-based dates into datetime objects.  

- **Example 2**:  
  - Create new columns, for instance:  
    - Weekly difference in prices (e.g., `RON95_price_diff` comparing consecutive weeks).  
    - Rolling averages (e.g., rolling mean of the last 4 weeks).  

Feel free to be creative: the key is to showcase your ability to manipulate and transform data in code.

---

## 3. Generating Insights

Using your transformed data, produce some insights or summary statistics. Below are a couple of examples, but you can decide what is most relevant:

1. **Example Insight 1**:  
   - Identify trends in RON95 vs. RON97 petrol prices over time.  
   - Possibly highlight the weeks with the largest jump in prices.

2. **Example Insight 2**:  
   - Compare diesel prices in Peninsular Malaysia vs. East Malaysia (if available in the dataset after June 2024, per the note in the data source).  
   - Show a simple time-series plot or bar chart of the difference.

The goal is to demonstrate that you can interpret data and produce visual or numerical insights.

---

## 4. Deliverables

1. **Google Colab (or Jupyter) Notebook**  
   - You must include all your code for:
     - Extraction (with retries, exceptions, logging).  
     - Transformation (in whatever approach you choose).  
     - Insight generation and visualization.
   - Make sure your notebook is well-documented (markdown cells explaining steps, code comments where needed).
   - Once the notebook is complete, download it as `.ipynb` and/or `.py` (if you have separate modules).

2. **Public GitHub Repository**  
   - Create a new **public** GitHub repository.  
   - Commit/push the notebook (and any associated code files) to this repository.
   - Share the link to the repository as your final submission.

---

### Tips for the Candidate

1. Keep your code organized and modular (e.g., separate your extraction logic from your transformations).  
2. Add helpful error messages and logging statements so anyone reading your logs can quickly understand what’s happening.  
3. Show at least one or two interesting plots or tables to highlight your insights.  
4. Ensure your GitHub repo is public so it can be accessed without special permissions.

---

## What We Will Be Evaluating

- **Correctness**: Can the script reliably pull data from the API?  
- **Robustness**: Have you implemented try-except blocks and retries to handle network/API issues?  
- **Code Readability**: Is your code well-organized, documented, and easy to follow?  
- **Data Transformation Skills**: Do you demonstrate proficiency in Pandas (or other libraries) to manipulate and prepare data?  
- **Analytical Thinking**: Are the insights or visualizations you produce meaningful or interesting?  
- **Version Control & Reproducibility**: Is the notebook reproducible on another machine, and is the GitHub repository well-structured?

---
