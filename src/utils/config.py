import configparser


class ConfigUtil:

    def __read_config_file(self, filename: str):
        self.config.read(filename)

    def get_section(self, section_name: str):
        return self.config[section_name]

    def get_option(self, section_name: str, option: str):
        return self.config.get(section_name, option)

    def __init__(self, filename):
        self.config = configparser.ConfigParser()
        self.__read_config_file(filename)
