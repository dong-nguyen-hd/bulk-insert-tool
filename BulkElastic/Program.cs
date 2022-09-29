using System.Text;
using System.Globalization;

namespace BulkElastic
{
    internal class Program
    {
        private static readonly string _uri = "http://localhost:9200/"; // Host default
        private static readonly string _index = "location"; // Index

        private static readonly string _username = "elastic";
        private static readonly string _password = "zBpIRYL-XOGsOgAx8SEq";

        private static readonly int _number = 20_000_000; // Total documents indexing
        private static readonly int _part = 20_000; // Length of part bulk-request

        private static Random _rand = new();

        private static List<string> _lngCoordinates = new List<string>();
        private static List<string> _latCoordinates = new List<string>();
        private static List<string> _addressDetail = new List<string>();

        private static List<string> _fuels = new List<string>() { "Hydro", "Solar", "Wind", "Gas", "Oil", "Coal", "Biomass", "Waste", "Nuclear", "Geothermal", "Cogeneration", "Storage", "Other", "Wave and Tidel", "Petcoke" };

        static async Task Main(string[] args)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Bulk query starting...");
            RenderData();

            for (int i = 0; i <= _number; i += _part)
            {
                string payload = RenderPayload(i);
                await ProcessRepositories(payload, i);
            }

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Complete Task!!!");
        }

        private static string RenderPayload(int number)
        {
            StringBuilder payload = new StringBuilder("");
            int count = _lngCoordinates.Count;
            for (int i = number; i <= _number; i++)
            {
                if ((i - number) == _part) break;

                string lng;
                string lat;
                string address;

                if (i >= count)
                {
                    var rndCoordinates = RandomCoordinates();
                    lng = rndCoordinates.lng.ToString();
                    lat = rndCoordinates.lat.ToString();
                    address = $"Address Test {i}";
                }
                else
                {
                    lng = _lngCoordinates[i];
                    lat = _latCoordinates[i];
                    address = _addressDetail[i];
                }

                payload.AppendLine($"{{\"index\":{{\"_index\":\"{_index}\"}}}}");

                string docPayload = $"{{\"id\": {i + 1}, \"fuel\": \"{RandomFuel()}\", \"number\": {_rand.Next()}, \"name\":\"Location {i + 1}\",\"address\": \"{address}\",\"coordinate\":{{\"lat\":{lat},\"lon\": {lng}}}}}";
                payload.AppendLine(docPayload);
            }

            payload.AppendLine("\n");

            return payload.ToString();
        }

        private static (double lng, double lat) RandomCoordinates()
        {
            var rndNumberDouble = _rand.NextDouble();
            var rndNumber = _rand.Next(0, _lngCoordinates.Count - 1);
            var lng = GetDouble(_lngCoordinates[rndNumber], 107) + rndNumberDouble;

            rndNumberDouble = _rand.NextDouble();
            rndNumber = _rand.Next(0, _latCoordinates.Count - 1);
            var lat = GetDouble(_latCoordinates[rndNumber], 16) + rndNumberDouble;

            return (lng, lat);
        }

        private static string RandomFuel()
        {
            var rndNumber = _rand.Next(0, _fuels.Count - 1);
            return _fuels[rndNumber];
        }

        private static double GetDouble(string value, double defaultValue)
        {
            double result;

            // Try parsing in the current culture
            if (!double.TryParse(value, NumberStyles.Any, CultureInfo.CurrentCulture, out result) &&
                // Then try in US english
                !double.TryParse(value, NumberStyles.Any, CultureInfo.GetCultureInfo("en-US"), out result) &&
                // Then in neutral language
                !double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out result))
            {
                result = defaultValue;
            }
            return result;
        }

        private static void RenderData()
        {
            _lngCoordinates.Clear();
            _latCoordinates.Clear();
            _addressDetail.Clear();

            using (var reader = new StreamReader(@".\poi_export_address.csv"))
            {
                int i = 0;
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();

                    if (i == 0) { i++; continue; }

                    var values = line?.Split(',');

                    if (values.Length >= 3)
                    {
                        _lngCoordinates.Add(values[0]);
                        _latCoordinates.Add(values[1]);
                        _addressDetail.Add($"Địa chỉ {i}"); // Adress
                    }

                    i++;
                }
            }
        }

        private static async Task ProcessRepositories(string value, int part)
        {
            HttpClientHandler clientHandler = new HttpClientHandler();
            clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };

            // Pass the handler to httpclient(from you are calling api)
            HttpClient client = new HttpClient(clientHandler);

            client.BaseAddress = new Uri(_uri);
            client.DefaultRequestHeaders.Accept.Clear();

            // Authentication
            var byteArray = Encoding.ASCII.GetBytes($"{_username}:{_password}");
            client.DefaultRequestHeaders.Add("Authorization", $"Basic {Convert.ToBase64String(byteArray)}");

            var httpContent = new StringContent(value, Encoding.UTF8, "application/json");

            // HTTP POST
            HttpResponseMessage response = await client.PostAsync($"{_index}/_bulk", httpContent);

            if (response.IsSuccessStatusCode)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"Successful: {part}/{_number}");
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Write($"Error: {part}/{_number}");
                Console.WriteLine(await response.Content.ReadAsStringAsync());
            }
        }
    }
}