from storage.storage_engine import FileStorage


if __name__ == "__main__":    
    file_storage = FileStorage()

    df = file_storage.load_dataframe("L", "campaign_subscriptions")
    rows, columns = df.shape
    print(f"Campaign Subscriptions Rows: {rows}, Columns: {columns}")
    print(f"{int(df.memory_usage(deep=True).sum() / 2014)}K")

    df = file_storage.load_dataframe("L", "campaign_tracks")
    print(f"Campaign Tracks Rows: {rows}, Columns: {columns}")
    print(f"{int(df.memory_usage(deep=True).sum() / 2014)}K")

    df = file_storage.load_dataframe("L", "tracks")
    print(f"Tracks Rows: {rows}, Columns: {columns}")
    print(f"{int(df.memory_usage(deep=True).sum() / 2014)}K")

    df = file_storage.load_dataframe("L", "nearest_edges")
    print(f"Nearest Edges Rows: {rows}, Columns: {columns}")
    print(f"{int(df.memory_usage(deep=True).sum() / 2014)}K")

    df = file_storage.load_dataframe("L", "way_shapes")
    print(f"Way Shapes Rows: {rows}, Columns: {columns}")
    print(f"{int(df.memory_usage(deep=True).sum() / 2014)}K")
