var fs = require('fs');

// Generate the built file.
buildTemplate('UtterancesTemplate.txt', 'built/SampleUtterances.txt');

/**
 * EchoQuery's very own Alexa utterances templating engine. There's a single
 * other package that does something like this called alexa-utterances. But,
 * it's pretty cumbersome, has a lot of unnecessary features, weird syntax, and
 * above all else, syntax that doesn't play nicely with custom slots.
 *
 * Here's the two major thing this thing does.
 *
 * [a|b|c] - Definite variation. An utterance will be made for each version
 *           listed here.
 *
 * Ex: "Hi [Gabe|Vinh]" => ["Hi Gabe", "Hi Vinh"]
 *
 * (a|b|c) - Optional variation. An utterance will be made for each version
 *           listed here, as well as one without.
 *
 * Ex: "Hi [Gabe|Vinh]" => ["Hi", "Hi Gabe", "Hi Vinh"]
 *
 * Additionally, supports, '\' Bash-like character used for line continuations.
 *
 * TODO(vqtran): Split this out into it's own NPM package and give it a cute
 *    name once we battle test this a bit.
 */

function buildTemplate(input, output) {
  // Read in input template file.
  fs.readFile(input, 'utf8', function(err, data) {
    if (err) {
      return console.log(err);
    }

    // Respect line continuations.
    data = data.replace(/\\\n/g, ' ');

    // Expand each line out into its combinations.
    var expandedUtterances = '';
    data.split('\n').forEach(function(line) {
      if (line == '' || line.charAt(0) == '#') {
        return;
      }
      expandedUtterances += expandUtterance(line).join('\n') + '\n';
    });

    // Write all expansions to new output file.
    fs.writeFile(output, expandedUtterances, function(err) {
      if (err) {
        return console.log(err);
      }
    });
  });
}

function expandUtterance(line) {
  // We'll keep a set of expansions that we'll constantly multiply in size as
  // we run into more variations.
  var expansions = [''];

  // Run through the line character by character and build the set. This could
  // probably done with Regex, but this might be easier to extend in the future
  // if we wanted to include more special template-y features.
  for (var i = 0; i < line.length; i++) {
    var current = line.charAt(i);

    if (current == '[') {
      // If we find a definite variation, grab everything until the next ending
      // bracket and extend the expansions.
      var j = i + line.substring(i).indexOf(']');
      var variations = line.substring(i+1, j).split('|');
      expansions = extendExpansions(expansions, variations);
      i = j;
    } else if (current == '(') {
      // If we find an optional variation, grab everything until the next ending
      // paranthesis and concat the new expansions to the existing ones to keep
      // the optional-ness of it.
      var j = i + line.substring(i).indexOf(')');
      var variations = line.substring(i+1, j).split('|');
      expansions = expansions.concat(extendExpansions(expansions, variations));
      i = j;
    } else {
      // Otherwise if its just a plain character then just expand by that.
      expansions = extendExpansions(expansions, [current]);
    }
  }

  // Clean up any extra whitespaces.
  expansions.forEach(function(line, i, arr) {
    arr[i] = line.trim().replace(/\s\s+/g, ' ');
  });

  return expansions;
}

// Extend an expansion set by the given variations, makes the new combinations.
function extendExpansions(expansions, variations) {
  var newExpansions = [];
  expansions.forEach(function(expansion) {
    variations.forEach(function(variation) {
      newExpansions.push(expansion + variation);
    });
  });
  return newExpansions;
}
