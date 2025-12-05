# agent_tools.py
from langchain_core.tools import tool
from service_interface import query_system_interface
from service_models import ServiceStatus
from typing import Union

@tool
def query_service_status(service_id: str) -> str:
    """
    Queries the external system to get the status of a specific service request.

    Use this tool to fetch details for a permit, license, or case when you have the service ID.

    Args:
        service_id: The unique identifier for the service request (e.g., "ACC-12345").

    Returns:
        A string containing the clean, validated service status information or an error message.
    """
    result: Union[ServiceStatus, str] = query_system_interface(service_id)
    
    # The agent will receive the string representation of the Pydantic model or the error string.
    return str(result)