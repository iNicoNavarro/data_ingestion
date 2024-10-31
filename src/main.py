from datetime import datetime, timedelta
import core
import pdfkit


PATH__JSON: str = '../json/'
PATH__PROFILE: str = '../profiling'
PATH__PARQUET: str = '../data/'
PATH__DB_SQLITE: str = '../db'

DATE_FORMAT: str = "%Y-%m-%d"
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 1, 31)


def fetch_and_upload(start_date, end_date):
    current_date = start_date
    week_start = current_date    
    weekly_data = []

    while current_date <= end_date:
        date_str = current_date.strftime(DATE_FORMAT)
        series_data = core.fetch_series_by_date(date_str)
    
        if series_data:
            weekly_data.extend(series_data)

        if current_date.weekday() == 6 or current_date ==end_date:
            week_end = current_date.strftime(DATE_FORMAT)
            filename = f"series_{week_start.strftime(DATE_FORMAT)}_{week_end}.json"
            core.save_json(
                data=weekly_data, 
                filename=filename,
                folder=PATH__JSON
            )
            weekly_data = []
            week_start = current_date + timedelta(days=1)
        current_date += timedelta(days=1) 


def main():
    # fetch_and_upload(
    #     start_date=START_DATE, 
    #     end_date=END_DATE
    # )

    df_raw = core.load_and_normalize_json_files(PATH__JSON)

    # core.generate_profiling_report(
    #     df=df_raw,
    #     output_folder=PATH__PROFILE,
    #     filename="reporte_series_tv.html"
    # )

    df_cleaned = core.clean_dataframe(df=df_raw)
    df_cleaned.head(10).to_csv('df_cleaned.csv', index=False)

    # core.save_to_parquet(
    #     df=df_cleaned, 
    #     folder=PATH__PARQUET, 
    #     filename="series_tv_data.parquet"
    # )

    core.create_database(
        db_path=PATH__DB_SQLITE,
        db_name='series_tv.db'
    )

    core.load_data_to_sqlite(
        parquet_path=PATH__PARQUET, 
        db_path=PATH__DB_SQLITE,
        db_name='series_tv.db'
    )


if __name__ == "__main__":
    main()
