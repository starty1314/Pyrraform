import configparser
import logging
import inspect
import os
import sys
from exceptions import ParameterTypeException, \
                       ParameterRangeException, \
                       OptionNotFoundException, \
                       SectionNotFoundException


def option_loader(app_config: str, section: str, option: str):
    """
    Get specific option value from a section in an app configuration file, only supported up to two levels
    :param app_config: Path of the app configuration file
    :param section: Section name
    :param option: Item in section
    :return: Value of an option
    """
    config_parser = configparser.ConfigParser()
    config_parser.read(app_config)
    if config_parser.has_section(section):
        config = dict(config_parser.items(section))
        if option in config:
            value = config[option]
            return value
        else:
            raise OptionNotFoundException(f"No option [{option}] found in section [{section}].")
    else:
        raise SectionNotFoundException(f"No section [{section}] found in {app_config}.")


def section_loader(app_config: str, section: str):
    """
    Get configuration data from project .conf file
    :param app_config: Path of the app configuration file
    :param section: The section name that in the project .conf file
    :return: The configuration data in the section or false if section doesn't exist
    """
    config_parser = configparser.ConfigParser()
    config_parser.read(app_config)
    if config_parser.has_section(section):
        config = dict(config_parser.items(section))
    else:
        return False

    return config


def logger():
    # set up logging to file - see previous section for more details
    caller = inspect.stack()[1]
    log_format = f'%(asctime)s: %(name)-12s:%(funcName)-10s: %(levelname)-8s: %(message)s'
    date_format = '%m/%d/%Y %I:%M:%S %p %Z'
    logging.basicConfig(level=logging.DEBUG,
                        format=log_format,
                        datefmt=date_format,
                        filename='app.log')

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)

    # set a format which is simpler for console use
    formatter = logging.Formatter(log_format, date_format)

    # tell the handler to use this format
    console.setFormatter(formatter)

    # add the handler to the root logger
    logging.getLogger().addHandler(console)

    my_logger = logging.getLogger(caller.filename.split("\\")[-1])

    return my_logger


def retrieve_name(var):
    """
    Retrieve variable/parameter's name
    :param var: Variable
    :return: The name of the variable
    """
    callers_local_vars = inspect.currentframe().f_back.f_locals.items()
    return [var_name for var_name, var_val in callers_local_vars if var_val is var]


def parameter_check(parameter, desired_type, value_range: set = None):
    """
    Check input parameter if satisfying requirements
    :param parameter: Input parameter
    :param desired_type: Desired type of the input parameter
    :param value_range: Check if the value of parameter falls in the expected value range
    :return: Return default value for the input parameter if default_value is not None
             No return if default_value is set
    """
    if isinstance(parameter, desired_type) is False:
        raise ParameterTypeException(f"Parameter Type Exception: parameter's type is \"{type(parameter)}\". valid type is \"{desired_type}\"")

    if value_range is not None and not set(parameter).issubset(set(value_range)):
        raise ParameterRangeException(f"Invalid value in the input argument {parameter}, valid values are {value_range}")

    return parameter


def get_project_path():
    """
    Get current project path
    :return: Current project path
    """
    return os.path.dirname(sys.modules['__main__'].__file__)
