import firebase_admin 
import os, json
from dotenv import load_dotenv
from firebase_admin import credentials, firestore
import pandas as pd
from sklearn.preprocessing import RobustScaler

load_dotenv()
db_key=os.getenv('DB_KEY')

firebase_key=json.loads(db_key)
cred=credentials.Certificate(firebase_key)
firebase_admin.initialize_app(cred)

db=firestore.client()

# ---------------- LOAD DISTRICTS ----------------
def load_districts_from_excel(file_path):
    """Load all states and districts from the provided Excel file."""
    df = pd.read_excel(file_path)

    # Assuming file has columns like: "State" and "District"
    df = df.rename(columns=lambda x: x.strip().lower())  # normalize
    return df[["state", "district"]]

# ---------------- FETCH SCORES ----------------
def get_district_scores(df_districts):
    """Fetch all district priority scores from firestore."""
    results = []

    for _, row in df_districts.iterrows():
        state, district = row["state"], row["district"]
        articles_ref = db.collection("states").document(state).collection(district).stream()

        scores = [doc.to_dict().get("priority", 0) for doc in articles_ref if "priority" in doc.to_dict()]
        
        avg_score = sum(scores)/len(scores) if scores else 0
        results.append({"state": state, "district": district, "avg_score": avg_score})

    return pd.DataFrame(results)

# ---------------- SCALE SCORES ----------------
def scale_scores(df):
    """Apply robust scaling to normalize scores."""
    scaler = RobustScaler()
    df["scaled_score"] = scaler.fit_transform(df[["avg_score"]])
    return df

# ---------------- SAVE TO FIRESTORE ----------------
def save_scores(df):
    for _, row in df.iterrows():
        state_ref = db.collection("states").document(row["state"])
        safety_ref = state_ref.collection("safety_score").document(row["district"])

        safety_ref.set({
            "district": row["district"],
            "avg_score": row["avg_score"],
            "scaled_score": float(row["scaled_score"])
        })

# ---------------- MAIN ----------------
def main():
    districts_df = load_districts_from_excel("Districts.xlsx")
    df = get_district_scores(districts_df)

    if df.empty:
        print("⚠️ No data found in Firestore")
        return

    df = scale_scores(df)
    save_scores(df)

    print("✅ All district safety scores saved to Firestore.")

if __name__ == "__main__":
    main()
