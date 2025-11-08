import subprocess
import json
import requests
from kafka import KafkaProducer
import time
import sys

# --- CONFIGURATION ---
KAFKA_SERVER = 'localhost:9092'
RESULTS_TOPIC = 'simulation_results'
EUREKA_API_ENDPOINT = 'http://localhost:3000/generate'

def run_simulation_headlessly(js_code):
    """
    CRITICAL: You must implement this function yourself.
    It needs to run the SimPhy simulation script without a GUI and return the final results.
    The JavaScript code should print a single JSON string at the end.
    """
    # This is a placeholder. Replace 'simphy_runner' with the real command to run a script.
    with open("temp_sim.js", "w") as f:
        f.write(js_code)
    
    result = subprocess.run(
        ['simphy_runner', 'temp_sim.js'], # <-- REPLACE THIS COMMAND
        capture_output=True, text=True, check=True, timeout=30
    )
    return json.loads(result.stdout.strip())

def start_analysis(config):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("🚀 Runner started...")
    
    prompt_template = config['prompt']
    params = config['parameters']
    param1_name, param1_vals = list(params.items())[0]
    param2_name, param2_vals = list(params.items())[1]

    for p1_val in range(param1_vals['min'], param1_vals['max'] + 1, param1_vals['step']):
        for p2_val in range(param2_vals['min'], param2_vals['max'] + 1, param2_vals['step']):
            try:
                prompt = prompt_template.format(angle=p1_val, velocity=p2_val)
                response = requests.post(EUREKA_API_ENDPOINT, json={'prompt': prompt})
                response.raise_for_status()
                js_code = response.json()['script']
                
                # Add a line to the JS to print the final result as JSON
                js_code += "\nconsole.log(JSON.stringify({range: final_range, flight_time: total_time}));"

                sim_output = run_simulation_headlessly(js_code)
                
                final_data = {param1_name: p1_val, param2_name: p2_val, **sim_output}
                producer.send(RESULTS_TOPIC, value=final_data)
                print(f"✅ Sent: {final_data}")

            except Exception as e:
                print(f"🔥 Failed run for {p1_val}, {p2_val}: {e}")

    producer.flush()
    print("🏁 Runner finished.")

if __name__ == '__main__':
    # The dashboard will pass the config, this is for direct testing
    if len(sys.argv) > 1:
        config = json.loads(sys.argv[1])
    else:
        config = {
            "prompt": "A cannon firing a ball at {angle} degrees with {velocity} m/s.",
            "parameters": {
                "angle": {"min": 10, "max": 80, "step": 5},
                "velocity": {"min": 20, "max": 100, "step": 10}
            }
        }
    start_analysis(config)