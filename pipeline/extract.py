"""Extract pollution data for a device from the PM2.5 API."""

from requests import get


def get_history(device_id: str) -> dict:
    """Get device data from API."""
    response = get(
        f"https://pm25.lass-net.org/API-1.0.0/device/{device_id}/history/", timeout=30)
    if response.ok:
        data = response.json()
        if int(data["num_of_records"]) > 0:
            return data['feeds']
        return {"Error": "No records for this device."}
    return {"Error": "Status code not ok."}


if __name__ == "__main__":
    # Manual tests:
    print(get_history('08BEAC0A08AE'))
    print(get_history('08BEAC252CE2'))
    # print(get_history('08BEAC252BA0'))
    # print(get_history('08BEAC252B9A'))
    # print(get_history('08BEAC252936'))
    # print(get_history('08BEAC245F3C'))
    # print(get_history('08BEAC07D3FE'))
