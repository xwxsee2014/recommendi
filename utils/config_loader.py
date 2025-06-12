import yaml

class ConfigLoader:
    def __init__(self, config_paths):
        self.config_paths = config_paths

    def _merge_dicts(self, dict1, dict2):  
        """  
        递归合并两个字典  
        如果两个字典有相同的键，且值都是字典，则递归合并它们  
        否则，如果dict2中的键在dict1中不存在，直接添加到dict1中  
        """  
        merged = dict1.copy()  # 复制dict1作为合并后的字典  
        for key, value in dict2.items():  
            if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):  
                # 如果键在两个字典中都存在，且值都是字典，则递归合并  
                merged[key] = self._merge_dicts(merged[key], value)  
            else:  
                # 否则，直接合并键值对  
                merged[key] = value  
        return merged

    def load_config(self):
        config = {}
        for config_path in self.config_paths:
            with open(config_path, "r") as f:
                config = self._merge_dicts(config, yaml.safe_load(f))
        return config