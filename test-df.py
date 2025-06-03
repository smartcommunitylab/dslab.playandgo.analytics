from storage.storage_engine import FileStorage


if __name__ == "__main__":    
    file_storage = FileStorage()

    df = file_storage.load_dataframe("L", "campaign_subscriptions", "2025")
    rows, columns = df.shape
    print(f"Campaign Subscriptions Rows: {rows}, Columns: {columns}")
    print(f"{int(df.memory_usage(deep=True).sum() / 2014)}K")

    df = file_storage.load_dataframe("L", "campaign_tracks", "2025")
    rows, columns = df.shape
    print(f"Campaign Tracks Rows: {rows}, Columns: {columns}")
    print(f"{int(df.memory_usage(deep=True).sum() / 2014)}K")

    df = file_storage.load_dataframe("L", "tracks", "2025")
    rows, columns = df.shape
    print(f"Tracks Rows: {rows}, Columns: {columns}")
    print(f"{int(df.memory_usage(deep=True).sum() / 2014)}K")

    df = file_storage.load_dataframe("L", "nearest_edges", "2025")
    rows, columns = df.shape
    print(f"Nearest Edges Rows: {rows}, Columns: {columns}")
    print(f"{int(df.memory_usage(deep=True).sum() / 2014)}K")

    df = file_storage.load_dataframe("L", "way_shapes")
    rows, columns = df.shape
    print(f"Way Shapes Rows: {rows}, Columns: {columns}")
    print(f"{int(df.memory_usage(deep=True).sum() / 2014)}K")
