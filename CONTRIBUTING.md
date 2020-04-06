## CONTRIBUTING

Where it makes sense, raise GitHub issues for any questions so others can benefit from the discussion, or follow the links to [the DDD-CQRS-ES #equinox Slack channel](https://ddd-cqrs-es.slack.com/messages/CF5J67H6Z) above for quick discussions.

This is an Open Source project for many reasons; some central goals:

- quality dependency-free reference code (the code should be clean and easy to read; where it makes sense, components can be grabbed and cloned locally and used in altered form)
- optimal resilience and performance (getting performance right can add huge value for some systems)
- this code underpins non-trivial production systems (so having good tests is not optional for reasons far deeper than having impressive coverage stats)

We'll do our best to be accommodating to PRs and issues, but please appreciate that [we emphasize decisiveness for the greater good of the project and its users](https://www.hanselman.com/blog/AlwaysBeClosingPullRequests.aspx); _new features [start with -100 points](https://blogs.msdn.microsoft.com/oldnewthing/20090928-00/?p=16573)_.

Within those constraints, contributions of all kinds are welcome:

- raising [Issues](https://github.com/jet/equinox/issues) (including [relevant question-Issues](https://github.com/jet/equinox/issues/56)) is always welcome (but we'll aim to be decisive in the interests of keeping the list navigable).
- bugfixes with good test coverage are naturally always welcome; in general we'll seek to move them to NuGet prerelease and then NuGet release packages with relatively short timelines (there's unfortunately not presently a MyGet feed for CI packages rigged).
- improvements / tweaks, _subject to filing a GitHub issue outlining the work first to see if it fits a general enough need to warrant adding code to the implementation and to make sure work is not wasted or duplicated_:
- [support for new stores](https://github.com/jet/equinox/issues/76) that can fulfill the normal test cases.
- tests, examples and scenarios are always welcome; Equinox is intended to address a very broad base of usage patterns. Please note that the emphasis will always be (in order)
  1. providing advice on how to achieve your aims without changing Equinox
  2. how to open up an appropriate extension point in Equinox
  3. (when all else fails), add to the complexity of the system by adding API surface area or logic
