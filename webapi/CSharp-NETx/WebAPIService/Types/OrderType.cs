﻿using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace PowerApps.Samples.Types
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum OrderType
    {
        Ascending,
        Descending
    }
}
