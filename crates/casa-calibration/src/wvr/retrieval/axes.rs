// SPDX-License-Identifier: LGPL-3.0-or-later
//! Small symmetric covariance eigensystem for the WVR proposal distribution.

const ORDER: usize = 4;
type Matrix = [[f64; ORDER]; ORDER];

pub(super) fn principal_axes(mut covariance: Matrix, dimensions: usize) -> Matrix {
    let mut basis =
        std::array::from_fn(|row| std::array::from_fn(|column| f64::from(row == column)));
    tridiagonalize(&mut covariance, &mut basis, dimensions);
    diagonalize(&mut covariance, &mut basis, dimensions);
    let mut order = [0, 1, 2, 3];
    order[..dimensions].sort_by(|&left, &right| {
        covariance[left][left]
            .abs()
            .total_cmp(&covariance[right][right].abs())
    });
    std::array::from_fn(|axis| {
        let column = order[axis];
        let step = covariance[column][column].max(0.0).sqrt() * 0.1;
        std::array::from_fn(|row| basis[row][column] * step)
    })
}

fn tridiagonalize(matrix: &mut Matrix, basis: &mut Matrix, size: usize) {
    for column in 0..size - 2 {
        let first = column + 1;
        let head = matrix[first][column];
        let norm = (first + 1..size).fold(0.0_f64, |norm, row| norm.hypot(matrix[row][column]));
        if norm == 0.0 {
            continue;
        }
        let subdiagonal = -head.hypot(norm).copysign(head);
        let factor = (subdiagonal - head) / subdiagonal;
        let mut reflector = [0.0; ORDER];
        reflector[first] = 1.0;
        for row in first + 1..size {
            reflector[row] = matrix[row][column] / (head - subdiagonal);
            matrix[row][column] = 0.0;
            matrix[column][row] = 0.0;
        }
        matrix[column][first] = subdiagonal;
        matrix[first][column] = subdiagonal;
        let mut update = [0.0; ORDER];
        for row in first..size {
            update[row] = factor * dot(&matrix[row], &reflector, first, size);
        }
        let correction = -0.5 * factor * dot(&reflector, &update, first, size);
        for row in first..size {
            update[row] += correction * reflector[row];
        }
        for row in first..size {
            for col in first..=row {
                matrix[row][col] -= reflector[row] * update[col] + update[row] * reflector[col];
                matrix[col][row] = matrix[row][col];
            }
        }
        for row in &mut basis[..size] {
            let projection = factor * dot(row, &reflector, first, size);
            for col in first..size {
                row[col] -= projection * reflector[col];
            }
        }
    }
}

fn dot(left: &[f64; ORDER], right: &[f64; ORDER], start: usize, end: usize) -> f64 {
    (start..end).map(|index| left[index] * right[index]).sum()
}

fn diagonalize(matrix: &mut Matrix, basis: &mut Matrix, size: usize) {
    // Householder reduction followed by implicit QR with a trailing Wilkinson
    // shift. The rotation convention fixes eigenvector signs for seeded draws.
    for _ in 0..128 {
        for index in 1..size {
            let scale = matrix[index - 1][index - 1].abs() + matrix[index][index].abs();
            if matrix[index][index - 1].abs() < f64::EPSILON * scale {
                matrix[index][index - 1] = 0.0;
                matrix[index - 1][index] = 0.0;
            }
        }
        let Some(last) = (1..size).rfind(|&index| matrix[index][index - 1] != 0.0) else {
            break;
        };
        let first = (1..last)
            .rfind(|&index| matrix[index][index - 1] == 0.0)
            .unwrap_or(0);
        qr_sweep(matrix, basis, first, last);
    }
}

fn qr_sweep(matrix: &mut Matrix, basis: &mut Matrix, first: usize, last: usize) {
    let gap = (matrix[last - 1][last - 1] - matrix[last][last]) * 0.5;
    let edge = matrix[last][last - 1];
    let denominator = (gap.abs() + gap.hypot(edge)).copysign(gap);
    let shift = matrix[last][last] - edge * (edge / denominator);
    let pivot = matrix[first][first];
    let mut x = if f64::EPSILON * shift.abs() > pivot.abs() + matrix[first + 1][first].abs() {
        pivot
    } else {
        pivot - shift
    };
    let mut y = matrix[first + 1][first];
    for left in first..last {
        let right = left + 1;
        let (cosine, sine) = annihilate(x, y);
        for row in &mut matrix[first..=last] {
            rotate(row, left, right, cosine, sine);
        }
        let (upper, lower) = matrix.split_at_mut(right);
        for (a, b) in upper[left][first..=last]
            .iter_mut()
            .zip(&mut lower[0][first..=last])
        {
            (*a, *b) = (cosine * *a - sine * *b, sine * *a + cosine * *b);
        }
        for row in &mut *basis {
            rotate(row, left, right, cosine, sine);
        }
        if left > first {
            matrix[right][left - 1] = 0.0;
            matrix[left - 1][right] = 0.0;
        }
        if right < last {
            x = matrix[right][left];
            y = matrix[right + 1][left];
        }
    }
    for row in first + 1..=last {
        matrix[row - 1][row] = matrix[row][row - 1];
    }
}

fn rotate(row: &mut [f64; ORDER], left: usize, right: usize, c: f64, s: f64) {
    let (a, b) = (row[left], row[right]);
    row[left] = c * a - s * b;
    row[right] = s * a + c * b;
}

fn annihilate(x: f64, y: f64) -> (f64, f64) {
    if y == 0.0 {
        return (1.0, 0.0);
    }
    let length = x.hypot(y);
    let sign = if y.abs() > x.abs() { -y } else { x }.signum();
    (sign * x / length, -sign * y / length)
}
