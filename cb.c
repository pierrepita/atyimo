int calculate_h(char *bloom1, char *bloom2, int size) {
	int h =0;
	for (int i = 0; i < size; i++) {
		if (bloom1[i] == '1') {
			if (bloom2[i] == '1') {
				h++;
			}
		}
	}
	return h;
}
