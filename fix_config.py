import configparser
import shutil
import os

config_file = 'config/config.ini'

def fix_config_duplicates(file_path):
    if not os.path.exists(file_path):
        # Fallback for running inside config folder
        if os.path.exists('config.ini'):
            file_path = 'config.ini'
        else:
            print(f"File not found: {file_path}")
            return

    print(f"Scanning {file_path} for duplicates...")
    
    # Read raw lines to detect duplicates manually
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        lines = f.readlines()

    new_lines = []
    current_section = None
    seen_keys = set()
    
    duplicates_found = 0
    
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('[') and stripped.endswith(']'):
            current_section = stripped
            seen_keys = set()
            new_lines.append(line)
            continue
        
        # Check for key-value pair
        if '=' in stripped and not stripped.startswith('#') and not stripped.startswith(';'):
            key = stripped.split('=', 1)[0].strip()
            
            if key in seen_keys:
                print(f"Removing duplicate key '{key}' in section {current_section}")
                duplicates_found += 1
                continue
            
            seen_keys.add(key)
            new_lines.append(line)
        else:
            new_lines.append(line)

    if duplicates_found > 0:
        backup_path = file_path + '.bak'
        shutil.copy(file_path, backup_path)
        print(f"Backup saved to {backup_path}")
        
        with open(file_path, 'w', encoding='utf-8-sig') as f:
            f.writelines(new_lines)
        print(f"Fixed {duplicates_found} duplicates. Please check {file_path} to ensure values are correct.")
    else:
        print("No duplicates found.")

if __name__ == "__main__":
    fix_config_duplicates(config_file)
