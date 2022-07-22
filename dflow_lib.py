class DataIngestion:
    def parse_method(self, string_input):
        values = re.split(",",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        row = dict(
            zip(('Date', 'Open', 'High', 'Low', 'Close', 'Volume'),
                values))
        return row

    def fix_date(data):
        import datetime
        d = datetime.datetime.strptime(data['Date'], "%d/%m/%Y")
        data['Date'] = d.strftime("%Y-%m-%d")
        return data