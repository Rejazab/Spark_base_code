# Explanation of each file.

## log_processing.py
### Description
Extraction of logs from a file where we are interested in mid and conv rows.</br>
Both dictionary and list are used but the dictionary could be changed for a list if you look for better performance.

### Exemple of logs
<i><b>text.path.date.timestamp:-information-processid-info-mid=info;ref=info;...;amount=123456;...</b><br>
text.path.date.timestamp:Nam nec sapien nibh. Suspendisse aliquet, ligula ut tempor vulputate, mi enim elementum erat, at egestas odio nunc a sapien. Sed quis turpis ut augue tincidunt pulvinar id ac ipsum.<br>
text.path.date.timestamp:Etiam vel justo fringilla nisi efficitur porta in nec lectus. Mauris euismod, libero ut fringilla pretium, velit est condimentum lectus, eu hendrerit ligula mauris a ipsum.<br>
<b>text.path.date.timestamp:-information-processid-info-conv;b_alias;m_alias;...;r_label;r_detailed;...</b><br>
text.path.date.timestamp:In elementum ut enim ut convallis. Maecenas pulvinar sed magna non aliquam. Duis luctus nisi et metus porta, vel pellentesque eros tempus.<br>
text.path.date.timestamp:Sed fermentum maximus interdum. Integer congue elit sit amet est congue, at placerat ex euismod. Phasellus venenatis pellentesque quam quis ultricies.<br></i>

