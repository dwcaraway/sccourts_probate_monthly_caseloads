import json
import os


# --- Configuration ---
INPUT_FILE = os.path.join(os.path.dirname(__file__), 'topojson', 'counties-10m.json')
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), 'topojson', 'sc-counties-topo.json')

# Load the TopoJSON file
with open(INPUT_FILE, "r") as f:
    topojson_data = json.load(f)

# Extract the counties object
counties_obj = topojson_data["objects"]["counties"]
geometries = counties_obj["geometries"]

# Filter geometries for South Carolina (FIPS state code 45)
sc_geometries = [g for g in geometries if g.get("id", "").startswith("45")]

# Create a new TopoJSON structure
sc_topojson = {
    "type": "Topology",
    "transform": topojson_data.get("transform"),
    "arcs": topojson_data.get("arcs"),
    "objects": {
        "counties": {
            "type": "GeometryCollection",
            "geometries": sc_geometries
        }
    }
}

# Save the filtered TopoJSON
with open(OUTPUT_FILE, "w") as f:
    json.dump(sc_topojson, f)

print(f"Filtered TopoJSON saved to '{OUTPUT_FILE}' with {len(sc_geometries)} South Carolina counties.")
