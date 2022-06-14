import pandas as pd

import parameters as p


def main():
    dttm_start = pd.Timestamp.now()
    print(f"Project Start Time: "
          f"{dttm_start.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

    subroutine()

    dttm_finish = pd.Timestamp.now()
    dttm_delta = dttm_finish - dttm_start
    print(f"\n\nProject Finish Time: "
          f"{dttm_finish.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Project Run Time: "
          f"{dttm_delta.total_seconds()/3600:,.2f} Minutes")


def subroutine():
    pass


if __name__ == '__main__':
    main()
