_modified from [EmbeddedArtistry](https://embeddedartistry.com/blog/2017/08/04/a-github-pull-request-template-for-your-projects/)_

# Description

Thank you for your contribution to pycytominer!
Please _succinctly_ summarize your proposed change.
What motivated you to make this change?

In https://github.com/cytomining/pycytominer/pull/129 the default feature that distinguishes object was set to `ObjectNumber` instead of `Metadata_ObjectNumber` assuming the CellProfiler4 uses the former. While working on new datasets, I realized that this wasn't the case. 

In this PR, I set the default feature name to `ObjectNumber`. Both `cyto_utils/cells.SingleCells()` and `aggregate.aggregate()` will continue to allow user input name for this feature in case it is needed.

## What is the nature of your change?

- [x] Bug fix (fixes an issue).
- [ ] Enhancement (adds functionality).
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected).
- [ ] This change requires a documentation update.

# Checklist

Please ensure that all boxes are checked before indicating that a pull request is ready for review.

- [x] I have read the [CONTRIBUTING.md](CONTRIBUTING.md) guidelines.
- [x] My code follows the style guidelines of this project.
- [x] I have performed a self-review of my own code.
- [x] I have commented my code, particularly in hard-to-understand areas.
- [x] I have made corresponding changes to the documentation.
- [x] My changes generate no new warnings.
- [x] New and existing unit tests pass locally with my changes.
- [x] I have added tests that prove my fix is effective or that my feature works.
- [x] I have deleted all non-relevant text in this pull request template.
