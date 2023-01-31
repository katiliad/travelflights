import os
directory = "../output_file"
for filename in os.listdir(directory):
    if filename.endswith(".json"):
        with open(os.path.join(directory, filename), 'r') as file:
            for line in file:
                print(line)