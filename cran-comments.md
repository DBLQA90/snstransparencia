## R CMD check results

Current local development check:

```text
0 errors | 0 warnings | 0 notes
```

The package was also checked with:

```sh
R CMD check --as-cran
```

in a restricted sandbox. Remaining notes/errors in that sandbox were caused by
environment limitations, including no DNS/network access for URL checks and
missing optional check tools such as `pandoc` and HTML tidy.

## Test environments

- Local Ubuntu 24.04.4 LTS, R 4.5.3

## New submission

This is a new submission.

## Internet resources

The package accesses the public SNS Transparencia API only when users call
client methods that query or download remote data. Package examples are guarded
by the `SNSTRANSPARENCIA_RUN_EXAMPLES` environment variable so they do not
require internet access during CRAN checks by default.
