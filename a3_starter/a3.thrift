struct MapEntry {
  1: string value,
  2: i32 version
}
service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void propagateToBackup(1: string key, 2: MapEntry me);
  void dumpToBackup(1: map<string, MapEntry> myMap);
}