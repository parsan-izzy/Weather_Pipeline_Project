import unittest
from WEaTHEr import fetch_data, process_data  # Import functions from your main script

class TestWeatherPipeline(unittest.TestCase):
    
    def test_fetch_data(self):
        city = "Berlin"
        data = fetch_data(city)
        self.assertIsNotNone(data)  # Test that data is fetched correctly
        self.assertIn('name', data)  # Test that the city name is in the data
    
    def test_process_data(self):
        sample_data = {
            "name": "Berlin",
            "main": {"temp": 15},
            "weather": [{"description": "clear sky"}]
        }
        result = process_data(sample_data)
        self.assertEqual(result[0], "Berlin")  # Test city name
        self.assertEqual(result[1], 15)  # Test Celsius temperature

if __name__ == "__main__":
    unittest.main()

