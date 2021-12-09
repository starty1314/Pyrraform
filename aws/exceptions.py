class ProjectExceptions(Exception):
    """
    Exception class from which every exception in this library will derive.
    It enables other projects using this library to catch all errors coming
    from the library with a single "except" statement
    """
    pass


class ParameterRangeException(ProjectExceptions):
    """
    Raise when the value of the parameter falls outside the range
    """
    pass


class ConfigurationException(ProjectExceptions):
    """
    Raise when the configuration is wrong
    """
    pass


class ParameterTypeException(ProjectExceptions):
    """
    Raise when the type of the parameter is not expected
    """
    pass


class OptionNotFoundException(ProjectExceptions):
    """
    Raise when the option is not found in the section
    """
    pass


class SectionNotFoundException(ProjectExceptions):
    """
    Raise when the section is not found in the config file
    """
    pass


class NotFoundException(ProjectExceptions):
    """
    Raise when the target is not found
    """
    pass


class NotSupportException(ProjectExceptions):
    """
    Raise when the target is not supported
    """
    pass