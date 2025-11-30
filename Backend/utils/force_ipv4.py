# utils/force_ipv4.py
import socket

def patch_socket_ipv4():
    """
    Monkey-patch socket.getaddrinfo to filter out IPv6 addresses.
    This is required for environments like Render (Free Tier) that do not support IPv6
    but where DNS resolves to an IPv6 address (like Supabase).
    """
    old_getaddrinfo = socket.getaddrinfo

    def new_getaddrinfo(*args, **kwargs):
        responses = old_getaddrinfo(*args, **kwargs)
        # Filter for IPv4 (AF_INET) only
        return [response for response in responses if response[0] == socket.AF_INET]

    socket.getaddrinfo = new_getaddrinfo
