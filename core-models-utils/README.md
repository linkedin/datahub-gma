Module which has utilities for core models, particularly URNs.

This is a separate module from `core-models` due to a technical restriction. Everything in `core-models` is already
defined at LinkedIn, we have essentially forked it in open source. So when we build internally, we need to actually
ignore `core-models`, and use the internal definitions instead. So for anything that _isn't_ forked, i.e. these
utilities, it needs to go into a separate module, or it would break LinkedIn's build, as they would be "missing".
