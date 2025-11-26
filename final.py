from flask import Flask, request, jsonify
from flask_cors import CORS
from lxml import etree
import logging
import re
import requests
from datetime import datetime

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)

API_URL = "https://psi-api-cxgna3aqbvekg0hu.southeastasia-01.azurewebsites.net/api/IcsrFields"

nsmap = {"xsi": "http://www.w3.org/2001/XMLSchema-instance"}

def strip_xmlns(xml_raw: str) -> str:
    return re.sub(r'\sxmlns="[^"]+"', "", xml_raw)

def load_xpath_mapping_from_api():
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict):
        data = [data]
    xpath_map = {}
    for item in data:
        element_header = (item.get("elementHeader") or "").strip()
        raw_xpath = (item.get("xPath") or "").strip()
        if not element_header or not raw_xpath:
            continue
        is_repeatable = "[r]" in raw_xpath
        clean_xpath = raw_xpath.replace("[r]", "").strip()
        config = {"xpath": clean_xpath, "repeatable": is_repeatable}
        xpath_map.setdefault(element_header, []).append(config)
    return xpath_map

def parse_e2b_xml_with_xpath_bytes(xml_bytes, xpath_map):
    try:
        raw_xml = xml_bytes.decode("utf-8")
    except Exception:
        raw_xml = xml_bytes.decode("latin-1", errors="ignore")
    clean_xml = strip_xmlns(raw_xml)
    root = etree.fromstring(clean_xml.encode("utf-8"))
    result = {}
    for field_code, config_list in xpath_map.items():
        value_found = None
        for config in config_list:
            xpath = config["xpath"]
            is_repeatable = config["repeatable"]
            try:
                nodes = root.xpath(xpath, namespaces=nsmap)
                if nodes:
                    extracted = []
                    for node in nodes:
                        if isinstance(node, str):
                            extracted.append(node)
                        elif hasattr(node, "text") and node.text:
                            extracted.append(node.text)
                        else:
                            extracted.append("")
                    value_found = extracted if is_repeatable else extracted[0]
                    break
            except Exception as e:
                value_found = f"Error: {str(e)}"
        if value_found is None:
            result[field_code] = [] if config_list[0]["repeatable"] else ""
        else:
            result[field_code] = value_found
    return result

NUMERIC_KEYS = {"N.1.1":2,"C.1.3":1,"FDA.C.1.7.1":1,"C.1.8.2":1,"C.1.11.1":1,"C.2.r.4":1,"C.2.r.5":1,"C.3.1":1,"C.5.4":1,"FDA.C.5.5a":10,"FDA.C.5.5b":10,"FDA.C.5.6.r":10,"D.2.2a":5,"D.2.2.1a":3,"D.2.3":1,"D.3":6,"D.4":3,"D.5":1,"D.7.1.r.1b":8,"D.8.r.6b":8,"D.8.r.7b":8,"D.9.2.r.1b":8,"D.9.4.r.1b":8,"D.10.2.2a":3,"D.10.4":6,"D.10.5":3,"D.10.6":1,"D.10.7.1.r.1b":8,"D.10.8.r.6b":8,"D.10.8.r.7b":8,"E.i.2.1b":8,"E.i.3.1":1,"E.i.6a":5,"E.i.7":1,"F.r.2.2b":8,"F.r.3.1":1,"F.r.3.2":50,"G.k.1":1,"FDA.G.k.1.a":1,"G.k.2.3.r.3a":10,"G.k.4.r.1a":8,"G.k.4.r.2":4,"G.k.4.r.6a":5,"G.k.5a":10,"G.k.6a":3,"G.k.7.r.2b":8,"G.k.8":1,"G.k.9.i.3.1a":5,"G.k.9.i.3.2a":5,"G.k.9.i.4":1,"G.k.10.r":2,"FDA.G.k.10a":2,"FDA.G.k.12.r.2.r":1,"FDA.G.k.12.r.8":1,"FDA.G.k.12.r.10":1,"FDA.G.k.12.r.11.r":1,"H.3.r.1b":8}
ALPHANUMERIC_KEYS = {"N.1.2":100,"N.1.3":60,"N.1.4":60,"N.2.r.1":100,"N.2.r.2":60,"N.2.r.3":60,"C.1.1":100,"C.1.6.1.r.1":2000,"C.1.8.1":100,"C.1.9.1.r.1":100,"C.1.9.1.r.2":100,"C.1.10.r":100,"C.1.11.2":2000,"C.2.r.1.1":50,"C.2.r.1.2":60,"C.2.r.1.3":60,"C.2.r.1.4":60,"C.2.r.2.1":60,"C.2.r.2.2":60,"C.2.r.2.3":100,"C.2.r.2.4":35,"C.2.r.2.5":40,"C.2.r.2.6":15,"C.2.r.2.7":33,"FDA.C.2.r.2.8":100,"C.3.2":100,"C.3.3.1":60,"C.3.3.2":50,"C.3.3.3":60,"C.3.3.4":60,"C.3.3.5":60,"C.3.4.1":100,"C.3.4.2":35,"C.3.4.3":40,"C.3.4.4":15,"C.3.4.5":2,"C.3.4.6":33,"C.3.4.7":33,"C.3.4.8":100,"C.4.r.1":500,"C.5.1.r.1":50,"C.5.2":2000,"C.5.3":50,"D.1":60,"D.1.1.1":20,"D.1.1.2":20,"D.1.1.3":20,"D.1.1.4":20,"D.2.2b":50,"D.2.2.1b":50,"D.7.1.r.1a":4,"D.7.1.r.5":2000,"D.7.2":10000,"D.8.r.1":250,"D.8.r.2a":20,"D.8.r.2b":20,"D.8.r.3a":10,"D.8.r.3b":250,"D.8.r.6a":4,"D.8.r.7a":4,"D.9.2.r.1a":4,"D.9.2.r.2":250,"D.9.4.r.1a":4,"D.9.4.r.2":250,"D.10.1":60,"D.10.2.2b":50,"D.10.7.1.r.1a":4,"D.10.7.1.r.5":2000,"D.10.7.2":10000,"D.10.8.r.1":250,"D.10.8.r.2a":10,"D.10.8.r.2b":20,"D.10.8.r.3a":10,"D.10.8.r.3b":250,"D.10.8.r.6a":4,"D.10.8.r.7a":4,"FDA.D.11.r.1":10,"FDA.D.12":10,"E.i.1.1a":250,"E.i.1.2":250,"E.i.2.1a":4,"E.i.6b":50,"F.r.2.1":250,"F.r.2.2a":4,"F.r.3.3":50,"F.r.3.4":2000,"F.r.4":50,"F.r.5":50,"F.r.6":2000,"G.k.2.1.1a":20,"G.k.2.1.1b":20,"G.k.2.1.2a":10,"G.k.2.1.2b":250,"G.k.2.2":250,"G.k.2.3.r.1":250,"G.k.2.3.r.2a":10,"G.k.2.3.r.2b":100,"G.k.2.3.r.3b":50,"G.k.3.1":35,"G.k.3.3":60,"G.k.4.r.1b":50,"G.k.4.r.3":50,"G.k.4.r.6b":50,"G.k.4.r.7":35,"G.k.4.r.8":2000,"G.k.4.r.9.1":60,"G.k.4.r.9.2a":25,"G.k.4.r.9.2b":15,"G.k.4.r.10.1":60,"G.k.4.r.10.2a":25,"G.k.4.r.10.2b":15,"G.k.4.r.11.1":60,"G.k.4.r.11.2a":25,"G.k.4.r.11.2b":15,"G.k.5b":50,"G.k.6b":50,"G.k.7.r.1":250,"G.k.7.r.2a":4,"G.k.9.i.1":32,"G.k.9.i.2.r.1":60,"G.k.9.i.2.r.2":60,"G.k.9.i.2.r.3":60,"G.k.9.i.3.1b":50,"G.k.9.i.3.2b":50,"FDA.G.k.10.1":10,"G.k.11":2000,"FDA.G.k.12.r.3.r":7,"FDA.G.k.12.r.4":80,"FDA.G.k.12.r.5":80,"FDA.G.k.12.r.6":10,"FDA.G.k.12.r.7.1a":100,"FDA.G.k.12.r.7.1b":100,"FDA.G.k.12.r.7.1c":35,"FDA.G.k.12.r.7.1d":40,"FDA.G.k.12.r.7.1e":2,"FDA.G.k.12.r.9":100,"H.1":100000,"H.2":20000,"H.3.r.1a":4,"H.4":20000,"H.5.r.1a":100000}
ALPHABETIC_KEYS = {"C.2.r.3":2,"C.5.1.r.2":2,"E.i.1.1b":3,"E.i.9":2,"G.k.2.4":2,"G.k.3.2":2,"H.5.r.1b":3}
BOOLEAN_KEYS = ["C.1.6.1","C.1.7","C.1.9.1","FDA.C.1.12","D.7.1.r.3","D.7.1.r.6","D.7.3","D.9.3","D.10.7.1.r.3","E.i.3.2a","E.i.3.2b","E.i.3.2c","E.i.3.2d","E.i.3.2e","E.i.3.2f","FDA.E.i.3.2h","E.i.8","F.r.7","G.k.2.5","FDA.G.k.12.r.1"]
DATETIME_KEYS = ["N.1.5","N.2.r.4","C.1.2","C.1.4","C.1.5","D.2.1","D.6","D.7.1.r.2","D.7.1.r.4","D.8.r.4","D.8.r.5","D.9.1","D.10.2.1","D.10.3","D.10.7.1.r.2","D.10.7.1.r.4","D.10.8.r.4","D.10.8.r.5","E.i.4","E.i.5","F.r.1","G.k.4.r.4","G.k.4.r.5"]
BLANK_KEYS = ["BlankField","(Header)"]

VALIDATION_SCHEMA = {}
for key, max_len in NUMERIC_KEYS.items():
    VALIDATION_SCHEMA[key] = {"type": "numeric", "max_length": max_len}
for key, max_len in ALPHANUMERIC_KEYS.items():
    VALIDATION_SCHEMA[key] = {"type": "alphanumeric", "max_length": max_len}
for key, max_len in ALPHABETIC_KEYS.items():
    VALIDATION_SCHEMA[key] = {"type": "alphabetic", "max_length": max_len}
for key in BOOLEAN_KEYS:
    VALIDATION_SCHEMA[key] = {"type": "boolean", "max_length": None}
for key in DATETIME_KEYS:
    VALIDATION_SCHEMA[key] = {"type": "datetime", "max_length": None}
for key in BLANK_KEYS:
    VALIDATION_SCHEMA[key] = {"type": "blank", "max_length": None}

def is_numeric(v):
    if isinstance(v, int):
        return True
    if isinstance(v, str):
        return v.isdigit()
    return False

def is_alphanumeric(v):
    return isinstance(v, str)

def is_alphabetic(v):
    return isinstance(v, str) and v.isalpha()

def is_boolean(v):
    if isinstance(v, bool):
        return True
    if isinstance(v, str):
        return v.lower() in ["true","false","1","0","yes","no"]
    return False

def is_datetime(value):
    if not isinstance(value, str):
        return False
    for fmt in ["%Y%m%d%H%M%S","%Y-%m-%d","%Y%m%d"]:
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            pass
    return False

def is_blank(v):
    return v == "" or v is None

def validate_value(value, rule_type, max_length=None):
    if value in (None, ""):
        return True
    if isinstance(value, list):
        return all(validate_value(i, rule_type, max_length) for i in value)
    if max_length is not None and len(str(value)) > max_length:
        return False
    if rule_type == "numeric":
        return is_numeric(value)
    if rule_type == "alphanumeric":
        return is_alphanumeric(value)
    if rule_type == "alphabetic":
        return is_alphabetic(value)
    if rule_type == "boolean":
        return is_boolean(value)
    if rule_type == "datetime":
        return is_datetime(value)
    if rule_type == "blank":
        return is_blank(value)
    return True

def validate_json_dict(data):
    errors = []
    for key, rule in VALIDATION_SCHEMA.items():
        if key not in data:
            continue
        value = data[key]
        if is_blank(value):
            continue
        if not validate_value(value, rule["type"], rule["max_length"]):
            msg = f"ERROR: Field '{key}' has invalid data. Value: '{value}'."
            if rule["type"] == "numeric":
                msg += " Expected a Number."
            elif rule["type"] == "alphanumeric":
                msg += " Expected Text/Numbers."
            if rule["max_length"] and len(str(value)) > rule["max_length"]:
                msg += f" It exceeds the maximum length of {rule['max_length']} characters."
            errors.append(msg)
    return errors

@app.route("/api/process-e2b", methods=["POST"])
def process_e2b():
    files = request.files.getlist("file")
    if not files or len(files) == 0:
        return jsonify({"message": "No files uploaded."}), 400
    try:
        xpath_map = load_xpath_mapping_from_api()
    except Exception as e:
        logging.error(f"Failed to load XPath mapping: {e}")
        return jsonify({"message": "Failed to load XPath mapping from API.", "error": str(e)}), 502
    successful_conversions = []
    errors = []
    for f in files:
        filename = getattr(f, "filename", None)
        try:
            content = f.read()
        except Exception as e:
            errors.append({"filename": filename, "error": f"Failed reading uploaded file: {e}", "data": None})
            continue
        try:
            json_result = parse_e2b_xml_with_xpath_bytes(content, xpath_map)
        except Exception as e:
            logging.error(f"Parsing error for {filename}: {e}")
            errors.append({"filename": filename, "error": f"Parsing error: {str(e)}", "data": None})
            continue
        try:
            validation_errors = validate_json_dict(json_result)
        except Exception as e:
            validation_errors = [f"Validation routine error: {e}"]
        if validation_errors:
            errors.append({"filename": filename, "error": validation_errors, "data": json_result})
        else:
            successful_conversions.append({"filename": filename, "data": json_result})
    return jsonify({"successful_conversions": successful_conversions, "errors": errors}), 200

if __name__ == "__main__":
    app.run(port=5000, debug=True)
