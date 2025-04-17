# 📊 Project Special Ed – AI-Powered Student Performance Tracker

**App:** [special-ed.streamlit.app](https://special-ed.streamlit.app)  
**Repo:** [GitHub Repo](https://github.com/asieduofeijnr/special_ed)

---

## 🧠 Overview

**Project Special Ed** is a web-based AI assistant designed to streamline weekly student performance tracking. The platform allows educators and stakeholders to input assessment data and receive structured performance summaries, insights, and recommendations. Powered by advanced LLMs like OpenAI's GPT and Anthropic's Claude, the system offers intelligent feedback, optional background research, and convenient export features for documentation and reporting.

---

## 🎯 Objectives

- Build an intuitive tool for tracking student assessment data.
- Use LLMs to generate summaries and insights from raw input.
- Perform real-time calculations and highlight performance trends.
- Enable optional research with sourced background insights.
- Export results as timestamped `.txt` files and send via email.
- Design for modularity and future integrations (Excel, Notion, etc.).

---

## ✅ Features

### 🔧 Functional Requirements

| Feature            | Description                                                                 |
|--------------------|-----------------------------------------------------------------------------|
| **Data Input**     | Submit weekly assessment data (quizzes, homework, participation, etc.)     |
| **LLM Integration**| Generate structured summaries using GPT/Claude                              |
| **Score Calculation** | Compute totals, averages, and performance scores                        |
| **Insight Generation** | Suggest actions based on trends and student data                     |
| **Research Tool**  | Fetch background info on learning strategies, benchmarks, etc.              |
| **File Export**    | Save output to `.txt` with timestamp and formatted summaries                |
| **Structured Output** | Responses formatted using `Pydantic` for consistency                    |
| **Error Handling** | Handle incomplete inputs, invalid formats, and LLM errors                   |

### 🛠️ Non-Functional Requirements

- Python 3.10+
- Dependency management via `requirements.txt`
- Modular codebase (`main.py`, `tools.py`, etc.)
- CLI-based interaction (Streamlit UI optional)
- Virtual environment with `venv`
- Version-controlled with Git & GitHub

---

## 💻 Technologies Used

| Area              | Tools & Libraries                        |
|-------------------|-------------------------------------------|
| **Language**      | Python                                    |
| **LLMs**          | OpenAI (GPT), Anthropic (Claude)          |
| **Libraries**     | LangChain, Pydantic, Streamlit (optional) |
| **File Handling** | Local `.txt` export                       |
| **Version Control** | Git, GitHub                            |
| **Editor**        | Visual Studio Code                        |

---

## 🚀 Getting Started

### 📦 Installation

```bash
# Clone the repository
git clone https://github.com/asieduofeijnr/special_ed.git
cd special_ed

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
# special_ed
